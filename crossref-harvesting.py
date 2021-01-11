#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__description__ = " harvest Crossref database for references_to_DOI and journal names \
                  analogue harvesting of author names (family name, given name) and ORCIDs \
                  analogue harvesting ISSN of papers (paper_id (=DOI), ISSN) "
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
        doi = row['doi']

        result = subprocess.run(f"{curl_command} https://api.crossref.org/works/{doi} --output tmp.json".split())
        logging.info(result)

        with jsonlines.open('./tmp.json') as reader:
            for obj in reader:

# harvest only references  from Crossref

                for reference in obj['message']['reference']:
                    reference_to = reference.get('DOI')
                    infos = doi, reference_to
                    print('INFOS:', infos)
                    csv_writer.writerow(infos)



# harvest publication title and ISSN from Crossref
                '''
                for paper in obj['message']['ISSN']:
                    issn = paper
                for titel  in obj['message']['title']:
                    title = titel

                    infos = doi, title, issn

                    print('paper_id, issn:', infos)
                    csv_writer.writerow(infos)
                

# harvest ISSN from Crossref
                
                for paper in obj['message']['ISSN']:
                    issn = paper
                    infos = doi, issn
                    print('paper_id, issn:', infos)
                    csv_writer.writerow(infos)
                

# harvest references and journal names from Crossref
                
                for reference in obj['message']['reference']:
                    reference_to = reference.get('DOI')
                    if 'journal-title' in reference.keys():
                        journal = reference['journal-title']
                    else:
                        journal = ('')
                    infos = doi, reference_to, journal
                    print('INFOS:', infos)
                    csv_writer.writerow(infos)
                
# harvest name (given, family) and ORCIDs from Crossref
                
                for author in obj['message']['author']:
                    given = author.get('given')
                    family = author.get('family')
                    name = family, given
                    if 'ORCID' in author.keys():
                        orcid = author['ORCID'].rsplit('/', 1)[1]
                    else:
                        orcid = ('')
                    infos = doi, name, orcid
                    print('INFOS', infos)
                    csv_writer.writerow(infos)
                '''

def main():
    parser = argparse.ArgumentParser(description=__description__)
    parser.add_argument('--curl_command', default='curl')
    parser.add_argument('input_file_csv') # needs to contain the DOIs for requesting references
    parser.add_argument('output_file_csv')
    args = parser.parse_args()

    with open(args.output_file_csv, 'a') as csvfile:
        csv_writer = csv.writer(csvfile)

# harvest Title, Publikationsdatum, ISSN-L
        #csv_writer.writerow(['paper_id', 'title', 'publ_date', 'ISSN-L'])


# harvest ISSN paper paper with paper_id (=DOI)
        #csv_writer.writerow(['paper_id','title', 'ISSN'])

# uncomment for harvest references and journal names from Crossref
        csv_writer.writerow(['paper_id', 'reference_to_doi'])

# harvest name (given, family) and ORCIDs from Crossref
        #csv_writer.writerow(['paper_id', 'authors', 'orcid'])
        col_list = ['doi']
        input = read_csv(args.input_file_csv, usecols=col_list)
        input = input.drop_duplicates()
        for index, row in input.iterrows():
            try:
                if row['doi']:
                    check_references(row, args.curl_command, csv_writer)
                    #print('main', row['doi'])

            except Exception as e:
                print ('Exception', e)

    print('done')
main()
