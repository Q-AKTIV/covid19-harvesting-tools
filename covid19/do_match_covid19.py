import pandas as pd
# DEBUG = 10000
DEBUG = None
print("loading orcid data")
df_orcid = pd.read_csv('authorship.csv', low_memory=False,
                       usecols=['orcid',  'doi'], nrows=DEBUG,
                       dtype={'orcid': str, 'doi': str})
print(df_orcid.head())
print("loading zbmedke covid19 data")
# paper.csv has columns: paper_id,doi, _, _
df_zbmedke = pd.read_csv("/mnt/covid19/ZBMED-COVID19-2020-v2/paper.csv", low_memory=False,
                         usecols=[0, 1], nrows=DEBUG,
                         names=['paper_id', 'doi'],
                         header=0,
                         dtype={0: str, 4: str, 5: str})


print("N ZB MED KE records:", len(df_zbmedke))
df_zbmedke_doi = df_zbmedke[df_zbmedke['doi'].notna() &
                            df_zbmedke['doi'].notnull()][['paper_id', 'doi']]
print("N ZB MED KE records with DOI:", len(df_zbmedke_doi))


del df_zbmedke

print("Filtering",len(df_orcid) ,"orcid records")
df_orcid = df_orcid[df_orcid.doi.isin(df_zbmedke_doi.doi)]
print(len(df_orcid) ,"orcid records have some match based on doi")

doi_matches = pd.merge(df_orcid, df_zbmedke_doi, on='doi',
                       how='inner')
print("DOI Matches", len(doi_matches))
print(doi_matches.head())
doi_matches = doi_matches[['paper_id', 'orcid']]


df =  doi_matches
print("All matches")
print(df)

tmp = len(df)
df.drop_duplicates(keep='first', inplace=True)
print("Dropped", len(df) - tmp, "duplicates")

# Cleanup
del  df_zbmedke_doi

print("Loading affiliations...")
df_affil = pd.read_csv("affiliation.csv",
                       dtype={'orcid': str, 'dis_org_id': str,
                              'dis_org_source': str})
print("Affiliation records:", len(df_affil))

print("Filtering affiliations...")
df_affil = df_affil[df_affil.orcid.isin(df.orcid)]
print("Kept", len(df_affil), "affiliation records")
print(df_affil.head())


# SAVE THINGS
if DEBUG is None:
    doi_matches.to_csv("authorship_matched_covid19-2020-06-24.csv", index=False)
    df_affil.to_csv("affiliation_matched_covid19-2020-06-24.csv", index=False)
