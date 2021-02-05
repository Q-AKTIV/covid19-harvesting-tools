#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import xmltodict
from collections import Counter

"""
SCHEMA:
{'VERFASSERANG', 'classification', 'bibkey', 'MedlineTANorm', 'TITLETRANSLAT',
 'EVENT', 'DATABASE', 'COLLECTIONTITLE', 'PAGES', 'ACCESS', 'PUBLDATE',
 'ABSTRACT', 'ISSN', 'SUBJECTCODE', 'SWD_norm', 'ARTICLELANGUAGE',
 'ZBMED_OPAC_KATKEY', 'PUBLISHER', 'ISSUE', 'PUBLCOUNTRY', 'MedlineTA',
 'DOCUMENTURL', 'TITLE', 'SERIENTITEL', 'ZDBID', 'MESH', 'sortyear',
 'TITLEVARIANT', 'EISSN', 'MOREPERSONS', 'REMARK', 'PUBLPERIOD', 'CHEM',
 'PUBLYEAR', 'INSTITCONGR', 'NOT_norm', 'UNTERTITEL', 'PER_norm', 'ISBN',
 'LISTTAG', 'PISSN', 'UMFANG', 'DBDOCTYPE', 'VOLUME', 'KOR_norm',
 'COLLECTIONSORT', 'COLLECTIONID', 'DOCTYPE', 'DOI', 'SOURCE', 'AUSGABE',
 'KEYWORDS', 'IDENTIFIER', 'AUTHOR', 'NOTE', 'INSTITUTION', 'SIGNATURE',
 'RECORDDATE', 'HBZID', 'LANGUAGE', 'PUBLPLACE', 'SUBDATABASE', 'ZSTA',
 'DBRECORDID', 'HERAUSGEBER'}
"""


def split_field(raw_string):
    return [t.strip() for t in raw_string.split(';')]


parser = argparse.ArgumentParser()
parser.add_argument("infile", nargs='+')
parser.add_argument("--mesh", help='Consider only mesh-annotated docs',
                    default=False, action='store_true')
parser.add_argument("--recent", help='Consider only 2020 records',
                    default=False, action='store_true')
parser.add_argument("--english", help='Consider only English records',
                    default=False, action='store_true')
args = parser.parse_args()

schema = set()


ntom_fields = ['classification', 'MESH', 'AUTHOR', 'KEYWORDS']
nto1_fields = ['SOURCE', 'LANGUAGE', 'DOCTYPE', 'DBDOCTYPE', 'sortyear']
fields = ntom_fields + nto1_fields

counters = {k: Counter() for k in fields}

num_records, num_valid = 0, 0
num_valid_has_mesh, num_valid_has_keywords = 0, 0
for infile in args.infile:
    print("Processing", infile)
    with open(infile, 'r') as xmlfile:
        records = xmltodict.parse(xmlfile.read())['zs:searchRetrieveResponse']\
            ['zs:records']

    for record in records['zs:record']:
        num_records += 1
        try:
            obj = record['zs:recordData']['doc']
        except KeyError:
            continue
        if args.recent:
            if 'sortyear' not in obj:
                continue
            if int(obj['sortyear']) != 2020:
                continue

        if args.english:
            if 'LANGUAGE' not in obj:
                continue
            if obj['LANGUAGE'] != 'eng':
                continue

        if args.mesh and 'MESH' not in obj:
            continue

        num_valid += 1
        if 'MESH' in obj:
            num_valid_has_mesh += 1
        if 'KEYWORDS' in obj:
            num_valid_has_keywords += 1
        schema |= set(obj.keys())

        for field in ntom_fields:
            if field in obj:
                counters[field].update(split_field(obj[field]))
        for field in nto1_fields:
            if field in obj:
                counters[field].update([obj[field]])

print("Schema:", schema, sep='\n')
print("Num         records", num_records)
print("Num   valid records", num_valid)
print("Num invalid records", num_records - num_valid)
print("Num valid with mesh:", num_valid_has_mesh)
print("Num valid with keywords:", num_valid_has_keywords)
for attribute, counter in counters.items():
    print("-" * 78)
    print("##", attribute)
    print("Num Classes:", len(counter))
    print("Num Labels:", sum(counter.values()))
    print("20 most common:", *counter.most_common(20), sep='\n\t')
    print("-" * 78)

print("ALL KEYWORDS:")
print(*counters['KEYWORDS'].most_common(), sep='\n\t')
