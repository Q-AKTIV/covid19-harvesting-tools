#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__description__ = "Harvest Konwledge Environment COVID19 data and retrieve affiliations from Wikidata "
__author__ = "Eva Seidlmayer <eva.seidlmayer@gmx.net>"
__copyright__ = "2020 by Eva Seidlmayer"
__license__ = "ISC license"
__email__ = "seidlmayer@zbmed.de"
__version__ = "1 "

import pandas as pd
import xml.etree.ElementTree as et
from SPARQLWrapper import SPARQLWrapper, JSON
import argparse
import csv

user_agent = "TakeItPersonally, https://github.com/foerstner-lab/TIP-lib"
wd_url = SPARQLWrapper("https://query.wikidata.org/sparql", agent=user_agent)

def main():
    parser = argparse.ArgumentParser(description=__description__)
    parser.add_argument("input")
    parser.add_argument("output")
    args = parser.parse_args()

    xtree = et.parse(args.input)
    xroot = xtree.getroot()

    docs = xroot.findall('.//doc')

    with open(args.output, 'a') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(['DBRECORDID','doi', 'title','author_qnr', 'author_affiliation'])

        for doc in docs:
            d = {key.tag: doc.find(key.tag).text for key in doc.iter() if key.tag != 'doc'}
            # doc_list.append(d)
            # print('keys', d.keys())

            try:
                doi = d['DOI']
                dbrecordid = d['DBRECORDID']
                title = d['TITLE']
                author_query = f'''SELECT distinct ?author 
                            WHERE {{ ?item wdt:P356 ?doi .    
                            Values ?doi {{ '{doi}' }}. 
                            {{ ?item wdt:P50 ?author }}. 
                            SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }}
                            }}'''
                wd_url.setQuery(author_query)
                wd_url.setReturnFormat(JSON)
                results = wd_url.query().convert()
                #print(results)
                if (len(results['results']['bindings'])) > 0:
                    for res in results['results']['bindings']:
                        author_qnr = res['author']['value'].rsplit('/', 1)[1]


                        singleemployer = []
                        aff_query = f"""SELECT distinct ?employer ?employerLabel   
                                    WHERE {{ VALUES ?item {{ wd:{author_qnr} }}.    
                                        ?item wdt:P108 ?employer .
                                    SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }} 
                                    }}"""
                        #print(aff_query)
                        wd_url.setQuery(aff_query)
                        wd_url.setReturnFormat(JSON)
                        aff_results = wd_url.query().convert()
                        if (len(aff_results["results"]["bindings"])) > 0:
                            for res in aff_results["results"]["bindings"]:
                                singleemployer.append(res["employerLabel"]["value"])
                                singleemployer.append(res["employer"]["value"].rsplit("/", 1)[1])
                                affiliation = singleemployer
                                infos = dbrecordid, doi, title, author_qnr, affiliation
                                print('1:',infos)
                                csv_writer.writerow(infos)
                        else:
                            affiliation = ''
                            infos = dbrecordid, doi, title, author_qnr, affiliation
                            print('2:', infos)
                            csv_writer.writerow(infos)
                else:
                    author_qnr = ''
                    affiliation = ''
                    infos = dbrecordid, doi, title, author_qnr, affiliation
                    print('3:', infos)
                    csv_writer.writerow(infos)
            except:
                pass

main()