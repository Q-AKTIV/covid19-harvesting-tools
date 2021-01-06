#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
File: extract_jsonl
Author: Lukas Galke
Email: python@lpag.de
Github: https://github.com/lgalke
Description: Extract pandas dataframe from jsonl file in LIVIVO format
Last Change: Mon Oct 21 2019 -- 16:16
"""

import argparse
import os
import zipfile
import jsonlines
import re
import sys

import pandas as pd
from tqdm import tqdm

def filter_by_top_counts(dataframe, col, keep):
    if isinstance(keep, float):
        assert keep > 0. and keep < 1.
        n_distinct = dataframe[col].nunique()
        keep = int(keep * n_distinct)
        print("Keeping {} of {} distinct {} values".format(keep, n_distinct, col))
    assert isinstance(keep, int), "Put int or float to make this work"
    top_items = pd.value_counts(dataframe[col], sort=True, ascending=False).index[:keep]
    return dataframe[dataframe[col].isin(top_items)]


def strip_mesh_qualifier(meshterm):
    """ Strip off qualifier terms from mesh terms,
    which are usually appended with a slash '/' """
    return meshterm.split('/')[0].strip()


def strip_qualifier_terms(meshterms):
    """
    Strip qualifier term(s) from meshterm that were appended via '/'.
    The result will contain each mesh term only once.
    """
    result = []
    for meshterm in meshterms:
        meshterm = strip_mesh_qualifier(meshterm)
        if meshterm not in result:
            result.append(meshterm)

    return result



def harvest_lines(lines_iter, filter_mesh_terms=None,
                  strict_mesh_filter=False,
                  filter_language=None, limit=None):
    json = jsonlines.Reader(lines_iter)
    flat_gen = ((d['_id']['$oid'], d['liv']['orig_data']) for d in json)
    papers = {}
    paper_author = []
    paper_keyword = []

    n_extracted = 0

    for identifier, data in tqdm(flat_gen):
        if limit and n_extracted > limit:
            break
        if "MESH" not in data:
            # There are no mesh terms at all!
            continue
        if filter_language:
            if "LANGUAGE" not in data:
                # Language not specified, drop record
                continue
            if filter_language not in data["LANGUAGE"]:
                # Record not in target language, drop record
                continue

        meshterms = strip_qualifier_terms(data['MESH'])

        if not meshterms:
            # There are no remaining meshterms, skip to next document
            continue

        if filter_mesh_terms is not None:
            if not any(mterm in filter_mesh_terms for mterm in meshterms):
                # No mesh term of document is included in target filter
                continue

        title = data.get('TITLE', [''])[0]

        papers[identifier] = [
            int(data['sortyear'][0]) if 'sortyear' in data else None,
            title,
            data['SOURCE'][0] if 'SOURCE' in data else None,
            data['DOI'][0] if 'DOI' in data else None,
            data['DBRECORDID'][1:] if data['DBRECORDID'].startswith('M') else None,
        ]

        if 'AUTHOR' in data:
            for author in data['AUTHOR']:
                paper_author.append((identifier, author))

        for meshterm in meshterms:
            # Add paper,meshterm tuples to table
            if filter_mesh_terms is not None and strict_mesh_filter:
                # Only if the term is in the filter!
                if meshterm in filter_mesh_terms:
                    paper_keyword.append((identifier, meshterm))
            else:
                paper_keyword.append((identifier, meshterm))

        n_extracted += 1
    print("Harvested metadata of %d papers" % n_extracted)
    df_paper = pd.DataFrame.from_dict(papers,
                                      orient='index',
                                      columns=['year',
                                               'title',
                                               'source',
                                               'doi',
                                               'pmId'])
    df_paper_keyword = pd.DataFrame(paper_keyword,
                                    columns=['paper_id', 'subject'])
    df_paper_author = pd.DataFrame(paper_author,
                                   columns=['paper_id', 'author'])

    return df_paper, df_paper_keyword, df_paper_author

def main():
    """ Extracts separate tables from jsonl ines file
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("jsonl_file", help="Input jsonlines file, may be zipped")
    parser.add_argument("-o", "--output", dest='save', default=None, type=str,
                        help="Save files in this directory")
    parser.add_argument("--normalize", default=False,
                        action='store_true',
                        help="Normalize string columns")
    parser.add_argument('--filter-language',
                        choices=["eng", "deu", "fra"],
                        help="Only harvest papers in specific language")
    parser.add_argument("--limit", type=int, default=None,
                        help="Parse fewer lines to create a debugging dataset")
    parser.add_argument("--filter-mesh", default=None, nargs='*',
                        help="Extract only documents, which are annotated with at least one of the MESH terms given in file (one per line)")
    parser.add_argument("--only-filtered-mesh", default=False, action='store_true',
                        help="Strictly keep only those mesh terms from filter.")
    parser.add_argument("--print-summary", default=False, action='store_true',
                        help="Print rough summary to file")
    parser.add_argument("--top-authors", default=None, type=float,
                        help="Fraction of top authors to consider")
    args = parser.parse_args()

    if args.filter_mesh is not None:
        print("Loading filter files")
        mesh_filter = set()
        for fpath in args.filter_mesh:
            with open(fpath, 'r') as fhandle:
                for line in fhandle:
                    mesh_filter.add(line.strip())
        print("Filtering for", len(mesh_filter), "mesh terms.")
    else:
        mesh_filter = None


    kwargs = {
        'limit': args.limit,
        'filter_language': args.filter_language,
        'filter_mesh_terms': mesh_filter,
        'strict_mesh_filter': args.only_filtered_mesh
    }

    if zipfile.is_zipfile(args.jsonl_file):
        with zipfile.ZipFile(args.jsonl_file, 'r') as z:
            # assume zip archive has only one file
            with z.open(z.infolist()[0]) as f:
                df_paper, df_paper_keyword, df_paper_author = harvest_lines(f, **kwargs)
    else:
        with open(args.jsonl_file, 'r') as f:
            df_paper, df_paper_keyword, df_paper_author = harvest_lines(f, **kwargs)

    if args.top_authors is not None:
        print("We have {} items in author-paper relation...".format(len(df_paper_author)))
        print("Keeping only top-{:2.0f}% authors".format(args.top_authors * 100))
        df_paper_author = filter_by_top_counts(df_paper_author, 'author', args.top_authors)
        print("Kept {} items in author-paper relation.".format(len(df_paper_author)))

    dfs = [
        # dataframe name, dataframe, whether to save index
        ('paper', df_paper, True),
        ('annotation', df_paper_keyword, False),
        ('authorship', df_paper_author, False)
    ]

    # Dumps everything to disk
    os.makedirs(args.save, exist_ok=True)
    for fname, dframe, save_index in dfs:
        dframe.to_csv(os.path.join(args.save, fname + '.csv'),
                      index=save_index)
    with open(os.path.join(args.save, 'args.txt'), 'w') as fh:
        print(args, file=fh)


if __name__ == "__main__":
    main()
