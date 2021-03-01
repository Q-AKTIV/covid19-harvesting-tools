import argparse
import os
from datetime import datetime

import pandas as pd

# List of date formats. Important: this has to go from more precise to less
INPUT_DATE_FORMATS= ["[%Y, %m, %d]", "%Y-%m-%d", "[%Y, %m]", "%Y-%m", "%Y"]
OUTPUT_DATE_FORMAT = "%Y-%m-%d"

# This needs to stay length two
DEFAULT_INPUT_DATE_COLUMN_NAMES = ['online_yyyy-mm-dd', 'print_yyyy-mm-dd']
OUTPUT_DATE_COLUMN_NAME = 'publdate'


# Print debug info
DEBUG = True

def precision_of_date_format(date_format):
    """ Analyze date format for its precision:
    Returns: 3 if the format contains year, month, and day.
    Returns: 2 if the format contains only year and month.
    Returns: 1 if the format contains only year.
    Otherwise: returns 0
    """
    precision = 0
    valuable_info = ["%Y", "%m", "%d"]
    for format_flag in valuable_info:
        if format_flag in date_format:
            precision += 1
    return precision

def convert_date(date_string: str, date_formats:[str]=INPUT_DATE_FORMATS) -> datetime:
    """ Try multiple date formats and return parsed date (datetime.datetime) for the first matching one"""
    assert isinstance(date_formats, list), "Please provide a list of formats"
    assert len(date_formats) > 0, "Please provide list of date formats"
    date_string = str(date_string)
    date = None
    prec = 0
    for format in date_formats:
        try:
            date = datetime.strptime(date_string, format)
            # Get precision of the date format that matched
            prec = precision_of_date_format(format)
            break
        except ValueError:
            pass
    return (date, prec)


def parse_date(date_string: str) -> str:
    """ Parses a single date into unified format """
    date, __prec = convert_date(date_string)
    if date is None:
        # No matching format
        return ""
    return date.strftime(OUTPUT_DATE_FORMAT)

def preferred_date(date_a_str: str, date_b_str: str) -> str:
    """ Parses two dates and outputs the earliest date with highest precision """
    date_a, prec_a = convert_date(date_a_str)
    date_b, prec_b = convert_date(date_b_str)
    if date_a is None and date_b is None:
        # Handle case when no date could be found in both strings
        return ""
    elif prec_a == prec_b:
        # If precisions are comparable, use the earlier date
        min_date = min(date_a, date_b)
    else:
        # If one date is more precise, ues this one
        min_date = max((date_a, prec_a), (date_b, prec_b), key=lambda x: x[1])[0]
    min_date_str = min_date.strftime(OUTPUT_DATE_FORMAT)
    if DEBUG:
        print(date_a_str, '|', date_b_str, '->', min_date_str)
    return min_date_str

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('input', help="Input csv file")
    parser.add_argument('--input_date_columns', nargs='+',
            default=DEFAULT_INPUT_DATE_COLUMN_NAMES,
            help="Specify date columns to parse, may be one or two")
    parser.add_argument('--keep_old_columns', action='store_true',
            default=False,
            help="Do *not* drop the old columns in the output.")
    parser.add_argument('-o', '--output', default=None,
            help="Path to write output")
    args = parser.parse_args()
    if args.output is None:
        print("No -o/--output given -> DRY RUN")
    else:
        if os.path.exists(args.output):
            answer = input(f"Output path exists: Overwrite '{args.output}' y/n?")
            if not answer.startswith('y'):
                exit(1)

    df = pd.read_csv(args.input)
    if len(args.input_date_columns) == 2:
        sub_df = df[args.input_date_columns]
        if DEBUG:
            print(sub_df.head())
        publdate = sub_df.apply(lambda row: preferred_date(row[0], row[1]), axis=1, result_type='reduce')
    elif len(args.input_date_columns) == 1:
        col = args.input_date_columns[0]
        publdate = df[col].map(parse_date)
    else:
        print("Invalid number of input date columns:", args.input_date_columns)

    if not args.keep_old_columns:
        df.drop(args.input_date_columns, axis=1, inplace=True)
    df[OUTPUT_DATE_COLUMN_NAME] = publdate

    if args.output:
        print("Writing output to", args.output)
        df.to_csv(args.output, index=False)
    # df['publdate'] = resolve_date

if __name__ == '__main__':
    main()
