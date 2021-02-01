# QAKTIV Harvesting Tools


## EconBiz

[API Documentation](https://api.econbiz.de/doc)

**Example:**

```
$ python3 fetch_econbiz.py -o tmp.json
Found 3241 records for query 'COVID-19'.
Fetching record data: 100%|████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 13/13 [00:04<00:00,  3.12it/s]
Saving records to 'tmp.json'.
```

## COVID+++
### retrival of publications from ZB MED-KE 
- annotated with keyword "covid19" in publication-year 2020
- 2021-02-01: number of 437073 papers (558392 without restriction on publication year 2020)
- see Shell-Script: SRU-KE_subject-covid19_date-2020.sh 
 
curl "http://z3950.zbmed.de:6210/livivo?version=2.0&operation=searchRetrieve&query=dc.subject=covid19%20AND%20dc.date=2020&facetLimit=0&startRecord=[1-558392:5000]&recordSchema=xml&x-username=zbmintern&maximumRecords=5000" -O "2021_ZBMED-COVID19-#1.xml" 
