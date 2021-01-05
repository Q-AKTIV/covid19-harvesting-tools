#!/usr/bin/bash
# 26.Mai: 77300 total number of results
# 27.Mai: 110429 total number of results
curl "http://z3950.zbmed.de:6210/livivo?version=2.0&operation=searchRetrieve&query=dc.subject=covid19+sortyear=2020&facetLimit=0&startRecord=[1-110429:5000]&recordSchema=xml&x-username=zbmintern&maximumRecords=5000" -o "tmp-ZBMED-COVID19-#1.xml"

# curl "http://z3950.zbmed.de:6210/livivo?version=2.0&operation=searchRetrieve&query=dc.subject=covid19&facetLimit=0&startRecord=[80001-110429:5000]&recordSchema=xml&x-username=zbmintern&maximumRecords=5000" -o "ZBMED-COVID19-#1.xml"


