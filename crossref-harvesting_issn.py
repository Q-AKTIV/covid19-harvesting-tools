#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__description__ = " harvest Crossref database for ISSN of publications (paper_id (=DOI), ISSN) "
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

# harvest ISSN from Crossref
                
                for paper in obj['message']['ISSN']:
                    issn = paper
                    infos = doi, issn
                    print('paper_id, issn:', infos)
                    csv_writer.writerow(infos)


def main():
    parser = argparse.ArgumentParser(description=__description__)
    parser.add_argument('--curl_command', default='curl')
    parser.add_argument('input_file_csv') # needs to contain the DOIs for requesting references
    parser.add_argument('output_file_csv')
    args = parser.parse_args()

    with open(args.output_file_csv, 'a') as csvfile:
        csv_writer = csv.writer(csvfile)


# uncomment for harvest ISSN from Crossref
        csv_writer.writerow(['paper_id', 'issn'])

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