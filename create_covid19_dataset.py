"""
This script combines the data for:
    - Publications
    - Preprints
    - References

"""
import argparse
import pandas as pd

def tee(*args, file=None):
    """Print to stdout and append to file (if not None)"""
    print(*args)
    if file is not None:
        with open(file, 'a') as fhandle:
            print(*args, file=fhandle)

class TrackChanges:
    """ Context Manager to track changes """
    def __init__(self, dataframe=None, desc="", logfile=None):
        self.desc = desc
        self.lens = []
        self.cols = []
        self.logfile = logfile
        self.dataframe = dataframe

    def __call__(self, df=None):
        if df is None:
            assert self.dataframe is not None
            df = self.dataframe
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

    def _pair_diffstr(self, pair):
        s = []
        if pair[0]:
            s.append(f"+{pair[0]}")
        if pair[1]:
            s.append(f"-{pair[1]}")
        return ' '.join(s)

    def _int_diff(self, numbers):
        if not numbers:
            return [], False
        current = numbers[0]
        diff = [current]
        has_changes = False
        for new in numbers[1:]:
            plusminus = new - current
            if plusminus != 0:
                has_changes = True
            diff.append(plusminus)
        return diff, has_changes

    def _tee(self, s):
        tee(s, file=self.logfile)

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
            lens_str = ' | '.join(f"{l} ({d:+d})" for l, d in zip(self.lens, lens_diff))
            s = f"[{self.desc}] Num records: {lens_str}"
            self._tee(s)
        elif self.lens:
            # No changes, still log number of elements
            s = f"[{self.desc}] Num records: {self.lens[0]}"
            self._tee(s)


def ensure_referential_integrity(df_a, df_b, left_col='paper_id', right_col=None,
                                 inplace=False):
    """ Ensures that col_a of df_a is in col_b of df_b, else drops """
    lhs = df_a[left_col]
    rhs = df_b.index if right_col is None else df_b[right_col]
    if not inplace:
        return df_a.drop(df_a[~lhs.isin(rhs)].index)
    df_a.drop(df_a[~lhs.isin(rhs)].index, inplace=True)

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


def load_dataframes(list_of_paths, **kwargs):
    """ Read multiple csv files as dataframes, **kwargs are passed down """
    print("Loading dataframes:", list_of_paths)
    dfs = []
    for path in list_of_paths:
        df = pd.read_csv(path, **kwargs)
        dfs.append(df)
    return dfs

def main():
    parser = argparse.ArgumentParser()
    # Papers (separate arg for each type, because different structure)
    parser.add_argument('--paper_data', nargs='+', required=True)
    parser.add_argument('--paper_data_sources', nargs='+', required=True)

    # Annotation (may be multiple files)
    parser.add_argument('--annotation_data', nargs='+', required=False,
                        default=[])

    # Authors (may be multiple files)
    parser.add_argument('--author_data', nargs='+', required=False,
                        default=[])

    # References (may be multiple files)
    parser.add_argument('--reference_data', nargs='+', required=False,
                        default=[])

    # Thresholds
    parser.add_argument('--min_papers_per_annotation', default=1, type=int)
    parser.add_argument('--min_papers_per_author', default=2, type=int)

    args = parser.parse_args()
    assert len(args.paper_data) == len(args.paper_data_sources), "Same number of paper data, and their source identifiers"

    paper_dfs = load_dataframes(args.paper_data)

    for df, source_identifier in zip(paper_dfs, args.paper_data_sources):
        assert 'data_source' not in df, "Data_source column already present"
        with TrackChanges(df, desc="Add data source identifier"):
            df['data_source'] = source_identifier

    df_paper = pd.concat(paper_dfs, ignore_index=True)
    # Combine papers
    with TrackChanges(df_paper, desc="Combine papers and drop duplicate DOIs"):
        assert all('paper_id' in df for df in paper_dfs)
        # Drop duplicates with descending priority: KE > PP > CR
        df_paper.drop_duplicates(subset="paper_id", keep="first", inplace=True)
        df_paper.set_index("paper_id", drop=True, append=False,
                           inplace=True, verify_integrity=True)

    if args.annotation_data:
        # Annotations
        annotation_dfs = load_dataframes(args.annotation_data)
        df_annotation = pd.concat(annotation_dfs, ignore_index=True)
        with TrackChanges(df_annotation, desc="Combine annotation data"):
            df_annotation.drop_duplicates(keep="first", inplace=True)
            df_annotation.reset_index(inplace=True)
        # TODO apply Tetyana Filter

    if args.author_data:
        # Authors
        author_dfs = load_dataframes(args.author_data, names=["paper_id","author", "orcid"])
        df_author = pd.concat(author_dfs, ignore_index=True)
        with TrackChanges(df_author, desc="Drop duplicate authors"):
            df_author.drop_duplicates(keep="first", inplace=True)
            df_author.reset_index(inplace=True)

        with TrackChanges(df_author, desc="Ref. Int. (authors)"):
            ensure_referential_integrity(df_author, df_paper, inplace=True)






if __name__ == '__main__':
    main()
