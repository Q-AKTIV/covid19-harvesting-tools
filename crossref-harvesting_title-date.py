#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__description__ = "harvest Crossref database for publication titles and publication date"
__author__ = "Eva Seidlmayer <seidlmayer@zbmed.de>"
__copyright__ = "2020 by Eva Seidlmayer"
__license__ = "ISC license"
__email__ = "seidlmayer@zbmed.de"
__version__ = "1 "


import argparse
import subprocess
from pandas import read_csv
import logging
import jsonlines
import csv


# curl is needed for the subprocess:
def check_references(row, curl_command, csv_writer):
        doi = row['paper_id']
        #doi = row['reference_to_doi']

        result = subprocess.run(f"{curl_command} https://api.crossref.org/works/{doi} --output tmp.json".split())
        logging.info(result)

        with jsonlines.open('./tmp.json') as reader:
            for obj in reader:


# harvest publication title and ISSN from Crossref
                for titel in obj['message']['title']:
                    title = titel
                try:
                    for date in obj['message']['published-online']['date-parts']:
                        online_date = date
                except:
                    online_date = ''
                try:
                    for date in obj['message']['published-print']['date-parts']:
                        print_date = date
                except:
                    print_date = ''

                infos = doi, title, online_date, print_date

                print('paper_id, issn, online_date, print_date:', infos)
                csv_writer.writerow(infos)


def main():
    parser = argparse.ArgumentParser(description=__description__)
    parser.add_argument('--curl_command', default='curl')
    parser.add_argument('input_file_csv') # needs to contain the DOIs for requesting references
    parser.add_argument('output_file_csv')
    args = parser.parse_args()

    with open(args.output_file_csv, 'a') as csvfile:
        csv_writer = csv.writer(csvfile)

# harvest Title, Publikationsdatum
        csv_writer.writerow(['paper_id', 'title', 'online_yyyy-mm-dd', 'print_yyyy-mm-dd'])

        col_list = ['paper_id']
        #col_list = ['reference_to_doi']
        input = read_csv(args.input_file_csv, usecols=col_list)
        input = input.drop_duplicates()
        for index, row in input.iterrows():
            try:
                if row['paper_id']:
                #if row['reference_to_doi']:
                    check_references(row, args.curl_command, csv_writer)

            except Exception as e:
                print('Exception', e)

    print('done')
main()
