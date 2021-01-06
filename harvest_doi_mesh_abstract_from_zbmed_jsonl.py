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
import json

import jsonlines
from tqdm import tqdm


def strip_mesh_qualifier(meshterm):
    """ Strip off qualifier terms from mesh terms,
    which are usually appended with a slash '/' """
    return meshterm.split('/')[0].strip()


def strip_qualifier_terms(meshterms):
    """
    Strip qualifier term(s) from meshterm that were appended via '/'.
    The result contains each mesh term only once.
    """
    return list(set(strip_mesh_qualifier(t) for t in meshterms))


def harvest_lines(lines_iter, min_year=None, filter_language=None, limit=None):
    json = jsonlines.Reader(lines_iter)
    # flat_gen = ((d['_id']['$oid'], d['liv']['orig_data']) for d in json)
    flat_gen = (d['liv']['orig_data'] for d in json)

    doi2mesh = {}
    doi2title = {}
    doi2abstract = {}

    n_extracted = 0

    for data in tqdm(flat_gen):
        # Do not exceed limit
        if limit and n_extracted > limit:
            break

        # Check for sortyear
        if "sortyear" not in data:
            continue
        year = int(data['sortyear'][0])
        if min_year:
            if year < min_year:
                continue

        if filter_language:
            if "LANGUAGE" not in data:
                # Language not specified, drop record
                continue
            if filter_language not in data["LANGUAGE"]:
                # Record not in target language, drop record
                continue


        if "MESH" not in data:
            # There are no mesh terms at all!
            continue
        meshterms = strip_qualifier_terms(data['MESH'])
        if not meshterms:
            # There are no remaining meshterms, skip to next document
            continue

        if "DOI" not in data:
            continue
        doi = data["DOI"][0]

        # Get title
        if "TITLE" not in data:
            continue
        title = data["TITLE"][0]

        if "ABSTRACT" not in data:
            continue
        abstract = data["ABSTRACT"][0]


        doi2mesh[doi] = meshterms
        doi2title[doi] = title
        doi2abstract[doi] = abstract

        n_extracted += 1
    print("Harvested metadata of %d papers" % n_extracted)

    return doi2mesh, doi2title, doi2abstract

def main():
    """ Extracts separate tables from jsonlines file """
    parser = argparse.ArgumentParser()
    parser.add_argument("jsonl_file", help="Input jsonlines file, may be zipped")
    parser.add_argument("-o", "--output", dest='save', default=None, type=str,
                        help="Save files in this directory")
    parser.add_argument('--filter-language',
                        choices=["eng", "deu", "fra"],
                        help="Only harvest papers in specific language")
    parser.add_argument("--limit", type=int, default=None,
                        help="Parse fewer lines to create a debugging dataset")
    parser.add_argument("--min-year", default=None, type=int,
                        help="Keep only those papers with year >= min_year")
    args = parser.parse_args()

    kwargs = {
        'limit': args.limit,
        'filter_language': args.filter_language,
        'min_year': args.min_year
    }

    if zipfile.is_zipfile(args.jsonl_file):
        with zipfile.ZipFile(args.jsonl_file, 'r') as zip_archive:
            # assume zip archive has only one file
            with zip_archive.open(zip_archive.infolist()[0]) as input_file:
                doi2mesh, doi2title, doi2abstract = harvest_lines(input_file, **kwargs)
    else:
        with open(args.jsonl_file, 'r') as input_file:
            doi2mesh, doi2title, doi2abstract = harvest_lines(input_file, **kwargs)

    # List of (File name w/o extension, dictionary to save)
    output_data = [
        ('doi2mesh', doi2mesh),
        ('doi2title', doi2title),
        ('doi2abstract', doi2abstract)
    ]

    # Dumps everything to disk
    os.makedirs(args.save, exist_ok=True)
    for fname, output_dict in output_data:
        path = os.path.join(args.save, fname + '.json')
        with open(path, 'w') as json_file:
            json.dump(output_dict, json_file)
    with open(os.path.join(args.save, 'args.txt'), 'w') as args_file:
        print(args, file=args_file)


if __name__ == "__main__":
    main()
