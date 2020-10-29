#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__description__ = " harvest Knowledge Environment Solr-Index database for DOI referenced by main publication "
__author__ = "Eva Seidlmayer <seidlmayer@zbmed.de>"
__copyright__ = "2020 by Eva Seidlmayer"
__license__ = "ISC license"
__email__ = "seidlmayer@zbmed.de"
__version__ = "1 "

import requests
import argparse
import csv
from pandas import read_csv



# harvest Knowldge Environment via Solr for referenced MESH-terms
def check_referenced_mesh(line, csv_writer):
    reference = line

#def check_referenced_mesh(row, csv_writer):
    #reference = row['reference_to_doi']
    r = requests.get(f'http://134.95.56.177:8080/solr/default/select?wt=json&q=DOI:{reference}')
    reference = r.json()
    print(reference)
    #print(reference['response']['docs'][0]['MESH'])
    year = reference['response']['docs'][0]['PUBLYEAR']
    date = reference['response']['docs'][0]['PUBLDATE']
    title = reference['response']['docs'][0]['TITLE']
    #referenced_mesh = reference['response']['docs'][0]['MESH']
    #for term in referenced_mesh:

        #infos = row['doi'], row['reference_to_doi'], term
    infos = line, title, year, date
    print(infos)
    csv_writer.writerow(infos)




def main():
    parser = argparse.ArgumentParser(description=__description__)
    parser.add_argument('input_file_csv') # needs to contain the DOIs for requesting references
    parser.add_argument('output_file_csv')
    args = parser.parse_args()

# creates different output files
    with open(args.output_file_csv, 'a') as csvfile:
        csv_writer = csv.writer(csvfile)
        #csv_writer.writerow(['paper_id', 'reference_to_doi', 'reference_to_MESH-terms'])
        #csv_writer.writerow(['reference_to_doi', 'reference_to_MESH-terms'])
        csv_writer.writerow(['paper_id', 'title','year','publ_date'])


# harvest input reference DOI
        input = read_csv(args.input_file_csv)
        input = input['reference_to_doi']
        input = input.drop_duplicates()
        for line in input:
            try:
                if line:
                #print(line)
                    check_referenced_mesh(line, csv_writer)
            except Exception as e:
                print('Exception', e)
     
        '''
        for index, row in input.iterrows():
            try:
                if row['reference_to_doi']:
                    #print('main', row['doi'], row['reference_to_doi'])
                    check_referenced_mesh(row, csv_writer)

            except Exception as e:
                print ('Exception', e)
        '''

main()
