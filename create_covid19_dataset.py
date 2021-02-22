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
    def __init__(self, dataframe=None, desc="", file=None):
        self.desc = desc
        self.lens = []
        self.cols = []
        self.file = file
        self.dataframe = dataframe

    def __call__(self, df):
        self.lens.append(len(df))
        self.cols.append(set(df.columns))
        return df

    def __enter__(self):
        if self.dataframe is not None:
            self(self.dataframe)
        return self

    def _set_diff(self, sets):
        if not sets:
            return [], False
        sets = [set(s) for s in sets]
        current = sets[0]
        diff = [(current, set())]  # list of sets
        has_changes = False
        for new in sets[1:]:
            plus = new - current
            minus = current - new
            diff.append((plus,minus))
            if plus or minus:
                has_changes = True
        return diff, has_changes

    def _pair_diffstr(self, d):
        if d[0] and d[1]:
            return f"+{d[0]} -{d[1]}"
        elif d[0]:
            return f"+{d[0]}"
        elif d[1]:
            return f"-{d[1]}"
        else:
            return ""

    def _int_diff(self, numbers):
        if not numbers:
            return [], False
        current = numbers[0]
        diff = [current]
        has_changes = False
        for new in numbers[1:]:
            plusminus = current - new
            if plusminus != 0:
                has_changes = True
            diff.append(plusminus)
        return diff, has_changes

    def _tee(self, s):
        print(s)
        if self.file is not None:
            with open(self.file, 'a') as logfile:
                print(s, file=logfile)

    def __exit__(self, type, value, trackback):
        if self.dataframe is not None:
            self(self.dataframe)
        # Column changes
        cols_diff, cols_have_changes = self._set_diff(self.cols)
        if cols_have_changes:
            cols_str = ' | '.join(self._pair_diffstr(d) for d in cols_diff)
            s = f"[{self.desc}] Columns: {cols_str}"
            self._tee(s)
        elif self.cols:
            # No changes, just log column set
            s = f"[{self.desc}] Columns: {self.cols[0]}"
            self._tee(s)

        # Number of records
        lens_diff, lens_have_changes = self._int_diff(self.lens)
        if lens_have_changes:
            lens_str = ' -> '.join(f"{l} ({d:+d})" for l, d in zip(self.lens, lens_diff))
            s = f"[{self.desc}] Num records: {lens_str}"
            self._tee(s)
        elif self.lens:
            # No changes, still log number of elements
            s = f"[{self.desc}] Num records: {self.lens[0]}"
            self._tee(s)


def ensure_referential_integrity(df_a, df_b, col_a='paper_id', col_b='paper_id'):
    """ Ensures that col_a of df_a is in col_b of df_b, else drops """
    with TrackChanges(desc="Ref. Int.") as tc:
        tc(df_a)
        lhs = df_a[col_a]
        rhs = df_b[col_b]
        df_a = df_a.drop(df_a[~lhs.isin(rhs)].index)
        tc(df_a)
    return df_a

def ensure_min_count_constraint(df: pd.DataFrame, col: str, threshold: int) -> pd.DataFrame:
    with TrackChanges(desc="Min Constr.") as tc:
        tc(df)
        counts = df[col].value_counts()
        invalid_values = counts[counts < threshold].index
        invalid_rows = df[df[col].isin(invalid_values)].index
        df = df.drop(invalid_rows)
        tc(df)
    return df

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
    parser.add_argument('--annotation_data', nargs='+', required=False)

    # Authors (may be multiple files)
    parser.add_argument('--author_data', nargs='+', required=False)

    # References (may be multiple files)
    parser.add_argument('--reference_data', nargs='+', required=False)

    # Thresholds
    parser.add_argument('--min_papers_per_annotation', default=1, type=int)
    parser.add_argument('--min_papers_per_author', default=2, type=int)

    args = parser.parse_args()

    df_ke = pd.read_csv(args.publ_papers)
    print("KE", df_ke.head())
    df_pp = pd.read_csv(args.preprint_papers)
    print("PP", df_pp.head())
    df_cr = pd.read_csv(args.referenced_papers)
    print("CR", df_cr.head())

    # Add data source
    with TrackChanges(df_ke, desc="Add data source: KE"):
        df_ke['data_source'] = "KE"

    with TrackChanges(df_pp, desc="Add data source: PP"):
        df_pp['data_source'] = "PP"

    with TrackChanges(df_cr, desc="Add data source: CR"):
        df_cr['data_source'] = "CR"

    with TrackChanges(desc="Combine papers and drop duplicate DOIs") as track:
        df_paper = pd.concat([df_ke, df_pp, df_cr], ignore_index=True)
        track(df_paper)
        # Drop duplicates with descending priority: KE > PP > CR
        df_paper.drop_duplicates(subset="paper_id", keep="first", inplace=True)
        track(df_paper)
        df_paper.set_index("paper_id", drop=True, append=False,
                           inplace=True, verify_integrity=True)
        track(df_paper)


if __name__ == '__main__':
    main()
