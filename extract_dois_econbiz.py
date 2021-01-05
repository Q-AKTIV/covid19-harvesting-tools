import argparse
import json

def is_doi(s):
    return s.startswith('https://doi.org/')

parser = argparse.ArgumentParser()

parser.add_argument('jsonfile', help="Input")

args = parser.parse_args()

with open(args.jsonfile, 'r') as file:
    data = json.load(file)




for obj in data:
    if 'identifier_url' in obj:
        for identifier in obj['identifier_url']:
            if is_doi(identifier):
                print(identifier)
