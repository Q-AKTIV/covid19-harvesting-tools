import argparse
import tarfile
import xmltodict
import re
from itertools import chain
from tqdm import tqdm
from collections import defaultdict
from joblib import Parallel, delayed
import pandas as pd
"""
A script for fast harvesting of orcid activity files (tar.gz)
Parallelized version could extract 2M affiliations in 10 minutes.

- lga
"""


WORKS_RE = re.compile(r".*/(.*)_works_\d*.xml")
AFFIL_RE = re.compile(r".*/(.*)_employments_\d*.xml")


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
    return (ext_id.get('common:external-id-type'),
            ext_id.get('common:external-id-value'))


def get_identifiers(orcid_work):
    """ Returns defaultdict filled with (type, value) tuples
    from an orcid work:work xml subtree"""
    if 'common:external-ids' in orcid_work and orcid_work['common:external-ids']:
        identifiers = maybe_map(get_single_identifier, orcid_work['common:external-ids'].get('common:external-id'))
    else:
        identifiers = []
    return defaultdict(str, identifiers)


def harvest_author_paper(path):
    author_paper = []
    with tarfile.open(path) as tf:
        for member in tqdm(tf, desc=path):
            if not member.isfile(): continue
            m = WORKS_RE.match(member.name)
            if m:
                orcid = m[1]
                xf = tf.extractfile(member)
                data = xmltodict.parse(xf.read())
                identifiers = get_identifiers(data['work:work'])
                if identifiers['pmid'] or identifiers['doi']:
                    row = orcid, identifiers['pmid'], identifiers['doi']
                    author_paper.append(row)
    return author_paper


def harvest_author_organization(path):
    author_organization = []
    with tarfile.open(path) as tf:
        for member in tqdm(tf, desc=path):
            if not member.isfile():
                continue
            # Handle works
            m = AFFIL_RE.match(member.name)
            if m:
                orcid = m[1]
                xf = tf.extractfile(member)
                data = xmltodict.parse(xf.read())
                try:
                    dis_org = data["employment:employment"]["common:organization"]["common:disambiguated-organization"]
                except KeyError:
                    # No disambiguated organization present
                    continue
                dis_org_id = dis_org["common:disambiguated-organization-identifier"]
                dis_org_source = dis_org["common:disambiguation-source"]
                row = orcid, dis_org_id, dis_org_source
                author_organization.append(row)
    return author_organization


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices={'authorship', 'affiliation', 'both'},
                        help="Extraction mode")
    parser.add_argument('tarfile', nargs='+',
                        help="Path to ORCID tar archives")
    parser.add_argument('-j', '--jobs', type=int, default=None,
                        help="How many parallel processes to use")
    args = parser.parse_args()
    n_jobs = len(args.tarfile) if args.jobs is None else args.jobs
    # tarfile_paths = glob.glob("*.tar.gz")
    print("Using {} jobs to harvest {} from tar files {}".format(n_jobs,
                                                                 args.mode,
                                                                 args.tarfile))
    if args.mode in ['authorship', 'both']:
        results = Parallel(n_jobs=n_jobs)(
            delayed(harvest_author_paper)(p) for p in args.tarfile)
        df_author_paper = pd.DataFrame(chain.from_iterable(results),
                                       columns=['orcid', 'pmid', 'doi'])
        df_author_paper.to_csv("authorship.csv", index=False)

    if args.mode in ['affiliation', 'both']:
        results = Parallel(n_jobs=n_jobs)(
            delayed(harvest_author_organization)(p) for p in args.tarfile)
        df_author_organization = pd.DataFrame(chain.from_iterable(results),
                                              columns=['orcid',
                                                       'dis_org_id',
                                                       'dis_org_source'])
        df_author_organization.to_csv("affiliation.csv", index=False)


if __name__ == '__main__':
    main()
