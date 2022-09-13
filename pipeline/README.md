# ONS Example Data Linkage Pipeline Guide

This readme will explain the data linkage scripts contained within this `pipeline/` directory.

## `00_CLEAN_CENSUS`
The clean census script reads in the raw census file and selects the required columns for linkage.

## `00_CLEAN_PES`
The clean PES script samples a subset of the census and can introduce simple errors into the data. Where the PES exists, this script would instead preprocess the PES.



<!-- TODO finish scripts below this line -->
## `01_WITHIN_HH`
Applies match keys within households

## `02_WITHIN_EA`
Applies match keys within enumeration areas (EA)


## `03_WITHIN_DISTRICT`
Applies match keys within districts


## `04_CLERICAL_SEARCH_EA`
Clerical search in the enumeration areas

## `05_WITHIN_COUNTRY`

## `06_POST_LINKAGE_QA`
Estimates the quality of the linkage and produces quality metrics


## `run_pipeline`
Runs the full pipeline from scripts `00` to `06`.




