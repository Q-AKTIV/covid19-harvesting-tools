#!/usr/bin/env python3
# -*- coding: utf8 -*-

"""

Harvests data from jsonl format with keys "id", "publdate", "title", "abstract", "entities", where the latter usually corresponds to MESH terms

- lga

"""

import os
import argparse
from collections import Counter
from collections import namedtuple

import jsonlines
from tqdm import tqdm
import pandas as pd


Publication = namedtuple('Publication', ['id', 'publdate', 'title', 'abstract'])
Annotation = namedtuple('Annotation', ['id', 'concept'])

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("infile")
    parser.add_argument("--save")

    args = parser.parse_args()

    paper_records, annot_records = [], []
    schema = Counter()
    with jsonlines.open(args.infile) as data:
        for i, obj in tqdm(enumerate(data), desc=args.infile):
            paper_records.append(
                Publication(obj["id"],
                            obj["date"],
                            obj["title"],
                            obj["abstract"])
            )

            # Make concepts unique!
            concepts = {entity['concept'] for entity in obj['entities']}

            # Add one annotation per unique concept
            annot_records.extend(
                [Annotation(obj["id"], concept) for concept in concepts]
            )

            schema.update(obj.keys())


    # Put records into dataframes
    df_paper = pd.DataFrame(paper_records, columns=Publication._fields)
    df_paper.set_index("id", inplace=True)
    df_annot = pd.DataFrame(annot_records, columns=Annotation._fields)

    if args.save:
        print("Saving results to", args.save)
        dfs = [
            # dataframe name, dataframe, whether to save index
            ('paper', df_paper, True),
            ('annotation', df_annot, False),
        ]

        # Dumps everything to disk
        os.makedirs(args.save, exist_ok=True)
        for fname, dframe, save_index in dfs:
            dframe.to_csv(os.path.join(args.save, fname + '.csv'),
                          index=save_index)
        with open(os.path.join(args.save, 'args.txt'), 'w') as fh:
            print(args, file=fh)
    else:
        print("Schema:", schema)


if __name__ == "__main__":
    main()
