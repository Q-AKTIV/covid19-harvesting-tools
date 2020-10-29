#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__description__ = " harvest Knowledge Environment Solr-Index database for numer of MESH term per year"
__author__ = "Klaus Lippert, Eva Seidlmayer <seidlmayer@zbmed.de>"
__copyright__ = "2020 by Eva Seidlmayer"
__license__ = "ISC license"
__email__ = "seidlmayer@zbmed.de"
__version__ = "1 "

import requests
import logger
import pysolr


solr = pysolr.Solr('http://134.95.56.177:8080/solr/default/')
print(solr.ping())  


# checks for amount of publications in the dataset related to specific MeSH terms 
terms = ['Ebolavirus',
         'Betacoronavirus',
         'Zika Virus',
         'SARS Virus',
         'Influenza A Virus, H5N1 Subtype',
         'African Swine Fever Virus',
         'Influenza A Virus, H1N1 Subtype',
         'HIV-1',
         'Middle East Respiratory Syndrome Coronavirus',
         'Marburgvirus']

years=['1994', '1993', '1992', '1991', '1990', '1989', '1988', '1987', '1986', '1985']
for year in years:
    for term in terms:
        number = solr.search('MESH:"{}" AND PUBLYEAR:"{}"'.format(term,year)).hits
        print(f'{year}{term}:', number)
    



# requests for Zika Virus in publication year 2002  
string ='http://134.95.56.177:8080/solr/default/select?q=MESH%3A%5B*+TO+*%5D+Zika+Virus%0APUBLYEAR%3A%5B*+TO+*%5D+2002&wt=json&indent=true'
r = requests.get(string)
print(r)
a=r.json()
print(a)


# requests for a specific DOI
r = requests.get('http://134.95.56.177:8080/solr/default/select?wt=json&q=DOI:10.1364/OE.20.000816')
print(r.url)
print(dir(r))
b=r.json()
print(b)


# requests for a bunch of DOI coming from a csv
reference_to = pd.read_csv('')
for doi in dois:
    r = requests.get(f'http://134.95.56.177:8080/solr/default/select?wt=json&q=DOI:{doi}')
    reference = r.json()
    print(reference)

