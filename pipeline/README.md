# ONS Example Data Linkage Pipeline Guide

This readme will detail the data linkage scripts contained within this `pipeline/` directory and how to run them. 

## Script running instructions
1. The census and PES file names should be updated in `lib/PARAMETERS.py`. Other key parameters can be configured.
2. Set the working directory to the parent folder `Rwandan_linkage` to ensure dynamic paths work correctly. If you experience issues with this then the line  `sys.path.insert(0, "../")` can be edited to include the filepath.
3. Run the `00_CLEAN_CENSUS` and `00_CLEAN_PES` cleaning scripts. You can add to these files any cleaning that may be required.
4. Run `01A_HH_MATCHKEYS`.
5. Resolve potential matches using CROW. To open CROW, run the file `CROW/CROW_clusters.py` and select the file to clerically investigate.
6. Once you have finished assessing the potential matches from CROW, remove your Windows username from the output file to create "Stage_1_Within_HH_Matchkey_Clerical_DONE.csv". 
7. Run `01B_HH_ASSOCIATIVE`
8. Run `02A_EA_MATCHKEYS`
9. Resolve matches in CROW, again removing your Windows username from the output file.
10. Run `02B_EA_ASSOCIATIVE`
11. Run `03A_EA_CLERICAL_MATCHKEYS`
12. Resolve matches in CROW.
13. Run `03B_EA_CLERICAL_MATCHKEYS_RESULTS`
14. Run `04A_CLERICAL_SEARCH_EA`
15. Resolve matches in CROW.
16. Run `04B_CLERICAL_SEARCH_EA_RESULTS`
17. Run `05A_DISTRICT_MATCHES`
18. Resolve matches in CROW.
19. Run `05B_DISTRICT_ASSOCIATIVE`
20. Run `06_COUNTRY`



## Script information 
### `00_CLEAN_CENSUS`
The clean census script reads in the raw census file and selects the required columns for linkage.

### `00_CLEAN_PES`
This script cleans and preprocesses the PES.

### `01A_HH_MATCHKEYS`
Applies match keys within households.
Following this script, clerical input using CROW is required.

### `01B_HH_ASSOCIATIVE`
Once clerical has been completed for households, this script will combine matches.

### `02A_EA_MATCHKEYS`
Applies match keys within enumeration areas (EA).
Following this script, clerical input using CROW is required.

### `02B_EA_ASSOCIATIVE`
Once clerical has been completed for EAs, this script will combine those matches.

### `03A_EA_CLERICAL_MATCHKEYS`
Broader matchkeys for clerical resolution in EAs.

### `03B_EA_CLERICAL_MATCHKEYS_RESULTS`
Combines EA clerical matches.

### `04A_CLERICAL_SEARCH_EA`
Clerical search in the enumeration areas
Following this stage, clerical resolution is required.

### `04B_CLERICAL_SEARCH_EA_RESULTS`
Combine matches found within enumeration areas.

###  `05A_DISTRICT_MATCHES`
Applies match keys within districts.
Following this script, clerical input using CROW is required.

### `05B_DISTRICT_ASSOCIATIVE`
Combine matches found in the district clerical resolution.


### `06_COUNTRY`
Applies match keys within country and combines previous matches.

### `07_POST_LINKAGE_QA`
Estimates the quality of the linkage and produces quality metrics.





