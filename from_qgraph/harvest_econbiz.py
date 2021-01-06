#!/usr/bin/env python3
# -*- coding: utf8 -*-

"""
File: extract_jsonl
Author: Lukas Galke
Email: python@lpag.de
Github: https://github.com/lgalke
Description: Extract pandas dataframe from jsonl files in EconBiz format
Last Change: Mon Jan 21 10:27:21 CET 2019
"""
import os
import argparse
import jsonlines
import pandas as pd
from tqdm import tqdm


def getsome(data, key):
    """ Extracts some item of a list for a key or None if not present """
    # TODO we could also get last
    return data.get(key, [None])[0]


def sanity_check(df):
    """ Manual sanity check by inspecting 5 samples """
    print(df.sample(5))


def main():
    """ Extract pandas dataframe from jsonl files in EconBiz format """
    parser = argparse.ArgumentParser()
    parser.add_argument('files', nargs='+')
    parser.add_argument('-o', '--output',
                        help="Store output tables in this directory")
    parser.add_argument('--only-annotated', default=False, action='store_true',
                        help="Store only annotated papers.")
    parser.add_argument('--filter-language',
                        choices=["eng", "deu", "fra"],
                        help="Only harvest papers in specific language")
    parser.add_argument("--normalize", default=False,
                        action='store_true',
                        help="Normalize string columns")
    parser.add_argument("--debug", type=int, default=None,
                        help="Parse fewer lines to create a debugging dataset")
    args = parser.parse_args()
    papers = {}
    paper_author = []
    paper_subject = []
    n_lines = 0
    n_with_annotations = 0
    for path in args.files:
        print("Harvesting", path)
        with jsonlines.open(path) as jsonfile:
            for jsonobj in tqdm(jsonfile):
                if args.debug is not None and n_lines > args.debug:
                    print("Collected %d lines, stopping." % n_lines)
                    break
                if args.filter_language:
                    if "language" not in jsonobj:
                        # Language not specified, drop record
                        continue
                    if args.filter_language not in jsonobj["language"]:
                        # Record not in target language, drop record
                        continue

                if args.only_annotated:
                    # If desired, we skip non-annotated papers.
                    if 'subject_stw' not in jsonobj or not jsonobj['subject_stw']:
                        # Paper has no annotations, continue
                        continue

                # Extract paper info
                identifier = jsonobj['econbiz_id']

                if 'date' in jsonobj:
                    date = jsonobj['date']
                else:
                    # Clear potential date of last iteration
                    date = None

                title = jsonobj['title']

                # if identifier in papers:
                #     raise UserWarning("Duplicate paper id: " + identifier)
                papers[identifier] = (date, title)
                # Extract author info
                # Use empty list if author not specified as key
                if 'creator_personal' in jsonobj:
                    authors = [a['name'] for a in jsonobj['creator_personal']]
                    for author in authors:
                        paper_author.append((identifier, author))

                if 'subject_stw' in jsonobj:
                    n_with_annotations += 1
                    for subject in jsonobj['subject_stw']:
                        paper_subject.append((identifier, subject["stw_id"]))
                # if 'subject_gnd' in jsonobj:  # THESE ARE AUTHORS!!!!!
                #     for subject in jsonobj['subject_gnd']:
                #         # GND subjects have 'gnd_id' and 'name'.
                #         # We use 'name' for now.
                #         paper_subject.append((identifier, subject['name']))
                n_lines += 1
    print("Found %d papers with %d authors and %d annotations."
          % (len(papers), len(paper_author), len(paper_subject)))
    print("%d papers do have stw annotations" % n_with_annotations)

    df_paper = pd.DataFrame.from_dict(papers,
                                       orient='index',
                                       columns=['year',
                                                'title'])
    df_paper_subject = pd.DataFrame(paper_subject,
                                    columns=['paper_id', 'subject'])
    df_paper_author = pd.DataFrame(paper_author,
                                   columns=['paper_id', 'author'])

    sanity_check(df_paper)
    sanity_check(df_paper_subject)
    sanity_check(df_paper_author)

    if args.normalize:
        print("Normalizing titles, authors, and subjects")
        from qgraph.utils import normalize_text
        for col in ['title']:
            df_paper[col] = df_paper[col].fillna('').apply(normalize_text)

        df_paper_author['author'] = df_paper_author['author']\
            .fillna('')\
            .apply(normalize_text)

        df_paper_subject['subject'] = df_paper_subject['subject']\
            .fillna('')\
            .apply(normalize_text)

    # dataframe name, dataframe, whether to save index
    dfs = [
        ('paper', df_paper, True),
        ('annotation', df_paper_subject, False),
        ('authorship', df_paper_author, False)
    ]
    print("Storing files to", args.output)
    os.makedirs(args.output, exist_ok=True)
    for fname, dframe, save_index in dfs:
        dframe.to_csv(os.path.join(args.output, fname + '.csv'),
                      index=save_index)


if __name__ == '__main__':
    main()
