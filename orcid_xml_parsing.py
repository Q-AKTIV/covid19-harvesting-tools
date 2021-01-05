"""
File: orcid_xml_parsing.py
Author: Lukas Galke
Email: git@lpag.de
Github: https://github.com/lgalke
Description: Parsing orcid xml files to pandas dataframes and csv tables
"""

import xmltodict
from collections import defaultdict

def maybe_map(apply_fn, maybe_list):
    """
    Applies `apply_fn` to all elements of `maybe_list` if it is a list,
    else applies `apply_fn` to `maybe_list`.
    Result is always list. Empty list if `maybe_list` is None.
    """
    if maybe_list is None:
        return []
    elif isinstance(maybe_list, list):
        return [apply_fn(item) for item in maybe_list]
    else:
        return [apply_fn(maybe_list)]

def get_single_identifier(ext_id):
    """ Returns (type, value) tuple) from a single external-id """
    return ext_id.get('common:external-id-type'), ext_id.get('common:external-id-value')

def get_identifiers_defaultdict(orcid_work):
    """ Returns defaultdict filled with (type, value) tuples from an orcid work """
    if 'common:external-ids' in orcid_work and orcid_work['common:external-ids']:
        identifiers = maybe_map(get_single_identifier, orcid_work['common:external-ids'].get('common:external-id'))
    else:
        identifiers = []
    return defaultdict(str, identifiers)

def get_publ_meta(orcid_work):
    """
    Arguments:
    - orcid_work: xml subtree (OrderedDict) corresponding to an orcid <work:work> tag
    Return: tuple of structured metaorcid_work for a single work
    """
    title = orcid_work['work:title'].get('common:title', '')
    subtitle = orcid_work['work:title'].get('common:subtitle', '')
    publ_year = orcid_work['common:publication-date'].get('common:year', None)
    publ_type = orcid_work.get('work:type', None)
    identifiers = get_identifiers_defaultdict(orcid_work)
    return (identifiers['pmid'], identifiers['doi'], publ_year, publ_type, title, subtitle)  


def get_auth_meta(orcid_work):
    identifiers = get_identifiers_defaultdict(orcid_work)
    orcid = orcid_work['common:source']['common:source-client-id'].get('common:path')
    return (identifiers['pmid'], orcid)


def main():
    import argparse
    import pandas as pd

    parser = argparse.ArgumentParser()
    parser.add_argument('xml_path', nargs='+')
    parser.add_argument('--save_publ_meta')
    parser.add_argument('--save_auth_meta')
    args = parser.parse_args()

    ##################################
    #  Extract publication metadata  #
    ##################################
    publ_records = []
    auth_records = []

    # TODO: We also need a mapping from authors to affiliatons

    for xml_path in args.xml_path:  # args.xml_path may be list of many xml pathes
        with open(xml_path, 'r') as fhandle:
            data = xmltodict.parse(fhandle.read())
        publ_records.extend(maybe_map(get_publ_meta, data['work:work']))
        auth_records.extend(maybe_map(get_auth_meta, data['work:work']))

    publ_df = pd.DataFrame.from_records(publ_records, columns=['pmid','doi','year','publ_type','title', 'subtitle'])
    auth_df = pd.DataFrame.from_records(auth_records, columns=['pmid','orcid'])
    print(publ_df)
    print(auth_df)

    if args.save_publ_meta:
        publ_df.tocsv(args.save_publ_meta)
    if args.save_auth_meta:
        publ_df.tocsv(args.save_auth_meta)


if __name__ == '__main__':
    main()




