"""
This script combines the data for:
    - Publications
    - Preprints
    - References

"""
import argparse
import pandas as pd
import os

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


def ensure_referential_integrity(df_a: pd.DataFrame, df_b: pd. DataFrame, left_col:str='paper_id', right_col=None,
        inplace:bool=False):
    """ Ensures that col_a of df_a is in col_b of df_b, else drops """
    lhs = df_a[left_col]
    rhs = df_b.index if right_col is None else df_b[right_col]
    if not inplace:
        return df_a.drop(df_a[~lhs.isin(rhs)].index)
    df_a.drop(df_a[~lhs.isin(rhs)].index, inplace=True)

def ensure_min_count_constraint(df: pd.DataFrame, col: str, threshold: int, inplace:bool=False) -> pd.DataFrame:
    counts = df[col].value_counts()
    invalid_values = counts[counts < threshold].index
    invalid_rows = df[df[col].isin(invalid_values)].index
    if not inplace:
        return df.drop(invalid_rows)
    df.drop(invalid_rows, inplace=True)

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
    parser.add_argument('--min_papers_per_annotation', default=None, type=int) # default 20
    parser.add_argument('--min_papers_per_author', default=None, type=int)  # default 2


    # OUTPUT
    parser.add_argument('-o', '--output', default=None, help="Write assembled dataset to this path.")  

    args = parser.parse_args()
    assert len(args.paper_data) == len(args.paper_data_sources), "Same number of paper data, and their source identifiers"

    if args.output:
        print("Creating output dir", args.output)
        os.makedirs(args.output, exist_ok=False)
        logfile = os.path.join(args.output, "assembly-log.txt")
    else:
        print("No output path given -> performing dry run")
        logfile = None

    ### PAPERS ###
    paper_dfs = load_dataframes(args.paper_data)
    for df, source_identifier in zip(paper_dfs, args.paper_data_sources):
        assert 'data_source' not in df, "Data_source column already present"
        with TrackChanges(df, desc="Add data source identifier", logfile=logfile):
            df['data_source'] = source_identifier

    df_paper = pd.concat(paper_dfs, ignore_index=True)
    # Combine papers
    with TrackChanges(df_paper, desc="Combine papers and drop duplicate DOIs", logfile=logfile):
        assert all('paper_id' in df for df in paper_dfs)
        # Drop duplicates with descending priority: KE > PP > CR
        df_paper.drop_duplicates(subset="paper_id", keep="first", inplace=True)
        df_paper.set_index("paper_id", drop=True, append=False,
                           inplace=True, verify_integrity=True)

    ### ANNOTATIONS ###
    if args.annotation_data:
        annotation_dfs = load_dataframes(args.annotation_data, names=["paper_id", "subject"],
                header=0)
        df_annotation = pd.concat(annotation_dfs, ignore_index=True)
        with TrackChanges(df_annotation, desc="Drop duplicates (annot)", logfile=logfile):
            df_annotation.drop_duplicates(keep="first", inplace=True)
            df_annotation.reset_index(inplace=True)
        with TrackChanges(df_annotation, desc="Remove qualifier terms", logfile=logfile):
            df_annotation.subject = df_annotation.subject.map(strip_qualifier)
        if args.min_papers_per_annotation:
            with TrackChanges(df_annotation, desc=f"Min papers per annotation: {args.min_papers_per_annotation}", logfile=logfile):
                ensure_min_count_constraint(df_annotation, "subject", args.min_papers_per_annotation)

    ### Author data
    if args.author_data:
        author_dfs = load_dataframes(args.author_data, names=["paper_id","author", "orcid"])
        df_author = pd.concat(author_dfs, ignore_index=True)
        with TrackChanges(df_author, desc="Drop NA authors", logfile=logfile):
            df_author.dropna(subset=["author"], inplace=True)
            df_author.reset_index(inplace=True)
        with TrackChanges(df_author, desc="Drop duplicate authors", logfile=logfile):
            df_author.drop_duplicates(keep="first", inplace=True)
            df_author.reset_index(inplace=True)
        with TrackChanges(df_author, desc="Ref. Int. (authors)", logfile=logfile):
            ensure_referential_integrity(df_author, df_paper, inplace=True)

        if args.min_papers_per_author:
            with TrackChanges(df_author, desc=f"Ensure min papers per author: {args.min_papers_per_author}", logfile=logfile):
                ensure_min_count_constraint(df_author, "author", args.min_papers_per_author, inplace=True)


    ### Reference data
    if args.reference_data:
        ref_dfs = load_dataframes(args.referenec_data, names=["paper_id","reference_to_doi"])
        df_refs = pd.concat(ref_dfs, ignore_index=True)
        with TrackChanges(df_refs, desc="Drop no target doi (refs)", logfile=logfile):
            df_refs.dropna(subset=["reference_to_doi"], inplace=True)
            df_refs.reset_index(inplace=True)
        with TrackChanges(df_refs, desc="Drop duplicate (refs)", logfile=logfile):
            df_refs.drop_duplicates(keep="first", inplace=True)
            df_refs.reset_index(inplace=True)
        with TrackChanges(df_author, desc="Ref. Int. (ref source)", logfile=logfile):
            ensure_referential_integrity(df_author, df_paper, left_col="paper_id", inplace=True)
        with TrackChanges(df_author, desc="Ref. Int. (ref target)", logfile=logfile):
            ensure_referential_integrity(df_author, df_paper, left_col="reference_to_doi", inplace=True)


    if not args.output:
        print("Dry run finished. Exiting.")

    print("Writing output to", args.output)
    df_paper.to_csv(os.path.join(args.output, "paper.csv"), index=True)
    df_author.to_csv(os.path.join(args.output, "authorship.csv"), index=False)
    df_annotation.to_csv(os.path.join(args.output, "annotation.csv"), index=False)
    df_refs.to_csv(os.path.join(args.output, "references.csv"), index=False)
    print("Done.")



if __name__ == '__main__':
    main()
