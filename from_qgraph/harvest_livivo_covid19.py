#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script for harvesting covid19 XML data

SCHEMA:
{'VERFASSERANG', 'classification', 'bibkey', 'MedlineTANorm', 'TITLETRANSLAT',
 'EVENT', 'DATABASE', 'COLLECTIONTITLE', 'PAGES', 'ACCESS', 'PUBLDATE',
 'ABSTRACT', 'ISSN', 'SUBJECTCODE', 'SWD_norm', 'ARTICLELANGUAGE',
 'ZBMED_OPAC_KATKEY', 'PUBLISHER', 'ISSUE', 'PUBLCOUNTRY', 'MedlineTA',
 'DOCUMENTURL', 'TITLE', 'SERIENTITEL', 'ZDBID', 'MESH', 'sortyear',
 'TITLEVARIANT', 'EISSN', 'MOREPERSONS', 'REMARK', 'PUBLPERIOD', 'CHEM',
 'PUBLYEAR', 'INSTITCONGR', 'NOT_norm', 'UNTERTITEL', 'PER_norm', 'ISBN',
 'LISTTAG', 'PISSN', 'UMFANG', 'DBDOCTYPE', 'VOLUME', 'KOR_norm',
 'COLLECTIONSORT', 'COLLECTIONID', 'DOCTYPE', 'DOI', 'SOURCE', 'AUSGABE',
 'KEYWORDS', 'IDENTIFIER', 'AUTHOR', 'NOTE', 'INSTITUTION', 'SIGNATURE',
 'RECORDDATE', 'HBZID', 'LANGUAGE', 'PUBLPLACE', 'SUBDATABASE', 'ZSTA',
 'DBRECORDID', 'HERAUSGEBER'}
"""

import os
import argparse
from collections import Counter
from tqdm import tqdm

import xmltodict
import pandas as pd

def strip_mesh_qualifier(meshterm):
    """ Strip off qualifier terms from mesh terms,
    which are usually appended with a slash '/' """
    return meshterm.split('/')[0].strip()

def sf(field):
    """ Split field """
    return field.split(' ; ')


def filter_min_count(paper_concept, min_count):
    __papers, concepts = zip(*paper_concept)
    counter = Counter(concepts)
    valid = {concept for concept, count in counter.most_common()
             if count >= min_count}
    return [(paper, concept) for (paper, concept) in paper_concept
            if concept in valid]



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("infile", nargs='+')
    parser.add_argument("--recent", help='Consider only 2020 records',
                        default=False, action='store_true')
    parser.add_argument("--require_publdate", help='Use only records with publdate',
                        default=False, action='store_true')
    parser.add_argument("--require_mesh", help='Use only publications with MESH terms, and only extract mesh terms ',
                        default=False, action='store_true')
    parser.add_argument("--require_doi", help='Use only publications with a DOI',
                        default=False, action='store_true')
    parser.add_argument("--strip-mesh-qualifiers",
                        help="Strip mesh qualifier terms (appended via '/')",
                        default=False, action='store_true')
    parser.add_argument("--save", help='Output directory')
    args = parser.parse_args()

    schema = Counter()

    num_records, num_valid = 0, 0

    papers = []
    paper_author = []
    paper_mesh = []

    for infile in args.infile:
        with open(infile, 'r') as xmlfile:
            xmldata = xmltodict.parse(xmlfile.read())
            try:
                records = xmldata['zs:searchRetrieveResponse']['zs:records']
            except KeyError:
                print(f"[skipping] '{infile}' does not match expected format, no 'searchRetrieveResponse' or no 'zs:records'")

        for record in tqdm(records['zs:record'], desc=infile):
            num_records += 1
            try:
                obj = record['zs:recordData']['doc']
            except KeyError:
                continue
            if args.recent:
                if 'sortyear' not in obj:
                    continue
                if int(obj['sortyear']) < 2020:
                    continue

            if args.require_publdate:
                if 'PUBLDATE' not in obj:
                    continue
                # Raise if cant be converted
                # tmp = pd.to_datetime(obj['PUBLDATE'], errors='raise', exact=False)
                # print(obj['PUBLDATE'], '=>', tmp)

            if args.require_mesh:
                if 'MESH' not in obj or not obj['MESH']:
                    # Skip, if no MESH or MESH empty
                    continue

            if args.require_doi:
                if "DOI" not in obj or not obj["DOI"]:
                    continue

            # Records
            num_valid += 1
            schema.update(obj.keys())

            # We use DOI as key
            # record_id = obj['DBRECORDID']
            doi = obj.get('DOI', '')
            title = obj.get('TITLE', '')
            date = sf(obj['PUBLDATE'])[0] if 'PUBLDATE' in obj else ''
            papers.append((doi, date, title))

            if 'AUTHOR' in obj:
                for author in sf(obj['AUTHOR']):
                    paper_author.append((doi, author))

            if 'MESH' in obj:
                meshterms = sf(obj['MESH'])
                if args.strip_mesh_qualifiers:
                    meshterms = [strip_mesh_qualifier(m) for m in meshterms]
                    # make unique after stripping qualifiers
                    meshterms = list(set(meshterms))

                for meshterm in meshterms:
                    paper_mesh.append((doi, meshterm))


    df_paper = pd.DataFrame(papers,
                            columns=['paper_id', 'publdate', 'title'])
    df_paper.set_index('paper_id', inplace=True)
    # Days granularity is enough
    # df_paper['publdate'] = pd.to_datetime(df_paper['publdate'],
    #                                       format='%Y-%m-%d', exact=False)
    df_paper['publdate'] = pd.to_datetime(df_paper['publdate'])
    print(df_paper.head())

    df_paper_concept = pd.DataFrame(paper_mesh,
                                    columns=['paper_id', 'concept'])
    print(df_paper_concept.head())

    df_paper_author = pd.DataFrame(paper_author,
                                   columns=['paper_id', 'author'])
    print(df_paper_author.head())



    if args.save:
        print("Saving results to", args.save)
        dfs = [
            # dataframe name, dataframe, whether to save index
            ('paper', df_paper, True),
            ('annotation', df_paper_concept, False),
            ('authorship', df_paper_author, False)
        ]

        # Dumps everything to disk
        os.makedirs(args.save, exist_ok=True)
        for fname, dframe, save_index in dfs:
            dframe.to_csv(os.path.join(args.save, fname + '.csv'),
                          index=save_index)
        with open(os.path.join(args.save, 'args.txt'), 'w') as fh:
            print(args, file=fh)
    print(schema)

if __name__ == "__main__":
    main()
