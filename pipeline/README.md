# ONS Example Data Linkage Pipeline Guide

This readme will explain the data linkage scripts contained within this `pipeline/` directory. Any key parameters can be configured in the `lib/PARAMETERS.py` file.

## `00_CLEAN_CENSUS`
The clean census script reads in the raw census file and selects the required columns for linkage.

## `00_CLEAN_PES`
This script cleans and preprocesses the PES.

## `01A_WITHIN_HH`
Applies match keys within households.
Following this script, clerical input using CROW is required.

## `01B_HH_Clerical`
Once clerical has been completed for households, this script will combine matches.

## `02A_WITHIN_EA`
Applies match keys within enumeration areas (EA).
Following this script, clerical input using CROW is required.

## `02B_EA_CLERICAL`
Once clerical has been completed for EAs, this script will combine those matches.

## `03A_WITHIN_DISTRICT`
Applies match keys within districts.
Following this script, clerical input using CROW is required.

## `03B_DISTRICT_CLERICAL`
Combine matches found in the district clerical resolution.

## `04A_CLERICAL_SEARCH_EA`
Clerical search in the enumeration areas
Following this stage, clerical resolution is required.

## `04B_CLERICAL_SEARCH_EA_CLERICAL`
Combine matches found within enumeration areas.

## `05_WITHIN_COUNTRY`
Applies match keys within country and combines previous matches.
## `06_POST_LINKAGE_QA`
Estimates the quality of the linkage and produces quality metrics.





