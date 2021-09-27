# COVID19 Harvesting Tools

This repository holds scripts for harvesting, merging, and cleaning COVID-19 research data.
It has been used to generate the **COVID19++ dataset**. A paper with more detailed description is coming soon.

## ZB MED Knowledge Environment

### Retrieval of publications from ZB MED-KE: Keyword=Covid-19 in year 2020

- annotated with keyword "covid19" in publication-year 2020
- 2021-02-01: number of 437073 papers (558392 without restriction on publication year 2020)
- see Shell-Script: SRU-KE_subject-covid19_date-2020.sh 
 
```
curl "http://z3950.zbmed.de:6210/livivo?version=2.0&operation=searchRetrieve&query=dc.subject=covid19%20AND%20dc.date=2020&facetLimit=0&startRecord=[1-558392:5000]&recordSchema=xml&x-username=zbmintern&maximumRecords=5000" -O "2021_ZBMED-COVID19-#1.xml" 
```
```
bash SRU-KE_subject-covid19_date-2020.sh
```

### Harvest retrieved **KE publications**


```
python3 from_qgraph/harvest_livivo_covid19.py /mnt/2021_covid++/ke_data/*.xml --recent --require_publdate --require_mesh --require_doi --strip-mesh-qualifiers --save /mnt/2021_covid++/ke_data_rel/
```

### Harvest retrieved **Preprints**

```
(venv) ubuntu@q-aktiv:~/git/harvesting-tools$ python3 from_qgraph/harvest_preprints_jsonl.py /mnt/2021_covid++/download --save /mnt/2021_covid++/preprints_rel_unmapped_mesh_ids
```

### Resolve MESH Identifiers from preprints to MeSH main subject headings (in text format)

- MeSH identifiers of preprints are mapped to MeSH descriptors (concepts) with the mesh_identifiers.R script.


### Filter relevant documents from retrieved **KE publications**
- Filter for **year**: 2020
- Filter for **doc_type**: "ARTIKEL"
- Filter for **db_doc_type**: "Case Reports","Clinical Conference","Clinical Study","Clinical Trial","Clinical Trial Protocol",
                        " Clinical Trial, Phase I"," Clinical Trial, Phase II"," Clinical Trial, Phase III","Consensus Development Conference",
                        "Controlled Clinical Trial","Dataset","Evaluation Study","Introductory Journal Article","Journal Article"


### Filter out non-specific mesh annotations 
- A list **nonspesific_mesh_annotations.txt** contains MeSH descriptors that are purposefully filtered out from the KE publications and Preprints data set, since they have too broad not specific meaning.


## CrossRef

### Get references for preprints and KE publications 

```
python3 crossref-harvesting.py input.csv output.csv
```

### Get ISSN for publications 

Needs to be performed two times for 'paper_id' and 'reference_to_doi'; the column needs to be changed manually in code 

Their are no ISSN for preprints as they are not published yet. 
```
python3 crossref-harvesting_issn.py KE-publ_ref.csv KE-publ-ref_issn.csv
```

### Get author names and ORCID-IDs for primary publications, references and preprints 

Needs to be performed two times for 'paper_id' and 'reference_to_doi'; the column needs to be changed manually in code (sorry)

```
 python3 crossref-harvesting_authors.py input.csv output.csv
```

### Get publication dates: online-publication-date and print-publication-date

```
python3 crossref-harvesting_title-date.py KE-publ_ref.csv KE-publ-ref_title-date.csv
```

## Get Metadata of Cited Work from  KE

### Harvesting MeSH-Terms for References 

This step Works only from ZB MED-VPN 

```
python3 analysis/KE-solr_harvester_reference-to.py  input.csv output.csv 
```

### Fix Dates

- Manually added header row to `KE-preprints-ref_title-date.csv`, then:

```bash
ubuntu@q-aktiv:~/git/harvesting-tools$ python3 resolve_publdate.py /mnt/2021_covid++/KE-preprints-ref_title-date_with_header.csv -o /mnt/2021_covid++/KE-preprints-ref_title-date_DATEFIX.csv 
```

## Assemble Dataset

``` 
BASEDIR=/mnt/2021_covid++
SCRIPTDIR=/home/ubuntu/git/harvesting-tools
SUBJ_THRESHOLD=20
AUTH_THRESHOLD=2
OUTPUT="/mnt/2021_covid++/covid19++-subj$SUBJ_THRESHOLD-auth$AUTH_THRESHOLD"
python3 $SCRIPTDIR/assemble_dataset.py --paper_data $BASEDIR/ke_data_rel/paper.csv $BASEDIR/preprints_rel/paper.csv $BASEDIR/KE-publ-ref_title-date_DATEFIX.csv $BASEDIR/KE-preprints-ref_title-date_DATEFIX.csv --paper_data_sources KE PP CR CR --annotation_data $BASEDIR/ke_data_rel/annotation.csv $BASEDIR/preprints_rel/annotation_mapped.csv $BASEDIR/KE-publ_ref_mesh.csv $BASEDIR/preprints_ref_mesh_202102.csv --author_data $BASEDIR/KE-publ_ref_authors.csv $BASEDIR/preprints_ref_authors.csv --reference_data $BASEDIR/KE-publ_ref.csv $BASEDIR/preprints_ref.csv --min_papers_per_annotation $SUBJ_THRESHOLD --min_papers_per_author $AUTH_THRESHOLD --output $OUTPUT
```

## Create graph from Dataset

```
(venv) ubuntu@q-aktiv:/mnt/2021_covid++$ ~/git/qgraph/bin/preprocess --add-references --authors include --ignore-title covid19++-subj20-auth2 -o covid19++-subj20-auth2-GRAPH
```
