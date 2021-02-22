"""
This script combines the data for:
    - Publications
    - Preprints
    - References

"""
import argparse
import pandas as pd

class TrackChanges:
    """ Context Manager to track changes """
    def __init__(self, desc="", file=None):
        self.desc = desc
        self.lens = []
        self.file = file

    def __call__(self, df):
        self.lens.append(len(df))

    def __enter__(self):
        self.lens = []

    def __exit__(self):
        changes = ' -> '.join(self.lens)
        s = f"[{self.desc}]: {changes}"
        print(s)
        if self.file is not None:
            with open(self.file, 'a') as logfile:
                print(s, file=logfile)

def ensure_referential_integrity(df_a, df_b, col_a='paper_id', col_b='paper_id'):
    """ Ensures that col_a of df_a is in col_b of df_b, else drops """
    with TrackChanges(desc="Ref. Int.") as tc:
        tc(df_a)
        lhs = df_a[col_a]
        rhs = df_b[col_b]
        df_a = df_a.drop(df_a[~lhs.isin(rhs)].index)
        tc(df_a)
    return df_a


def fix_publdate(df):
    assert 'publdate' in df
    with TrackChanges(desc="Publ. Date") as tc:
        tc(df)
        df = df.dropna(subset=['publdate'])
        df.publdate = df.publdate.str.rstrip('-')
        df.publdate = df.publdate.map(lambda s: s[:s.index('-')] if '--' in s else s)
        tc(df)
    return df

def strip_qualifier(s):
    return s if '/' not in s else [s.index('/')]


def main():
    parser = argparse.ArgumentParser()
    # Papers (separate arg for each type, because different structure)
    parser.add_argument('--publ_papers', required=True)
    parser.add_argument('--preprint_papers', required=True)
    parser.add_argument('--referenced_papers', required=True)

    # Annotation (may be multiple files)
    parser.add_argument('--annotation_data', nargs='+', required=True)

    # Authors (may be multiple files)
    parser.add_argument('--author_data', nargs='+', required=True)

    # References (may be multiple files)
    parser.add_argument('--reference_data', nargs='+', required=True)

    # Thresholds
    parser.add_argument('--min_papers_per_annotation', default=1, type=int)
    parser.add_argument('--min_papers_per_author', default=2, type=int)

if __name__ == '__main__':
    main()
