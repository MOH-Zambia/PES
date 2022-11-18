# Import any packages required
import pandas as pd
import numpy as np
import jellyfish
import os
import sys
sys.path.insert(0, "../")
from lib.PARAMETERS import *

# Functions
from lib.Cluster_Function import cluster_number

# Read in the census data
CEN = pd.read_csv(DATA_PATH + 'census_cleaned.csv', index_col=False)
print("Census read in")

# Read in the PES data
PES = pd.read_csv(DATA_PATH + 'pes_cleaned.csv', index_col=False)
print("PES read in")

# ----------------------------------------------------------------------- #
# ----------------- EXTRA WITHIN EA CLERICAL MATCHING  ------------------ #
# ----------------------------------------------------------------------- #    

# Read in all matches made so far
prev_matches = pd.read_csv(OUTPUT_PATH + 'Stage_2_All_Within_EA_Matches.csv')

# CEN residuals
CEN = CEN.merge(prev_matches[['puid_cen']], on='puid_cen', how='left', indicator=True)
CEN = CEN[CEN['_merge'] == 'left_only'].drop('_merge', axis=1)

# PES residuals
PES = PES.merge(prev_matches[['puid_pes']], on='puid_pes', how='left', indicator=True)
PES = PES[PES['_merge'] == 'left_only'].drop('_merge', axis=1)

# CLERICAL Matchkey 1: 
matches_1 = pd.merge(left=CEN,
                     right=PES,
                     how="inner",
                     left_on=['names_cen', 'EAid_cen'],
                     right_on=['names_pes', 'EAid_pes'])

# CLERICAL Matchkey 2:          
matches_2 = pd.merge(left=CEN,
                     right=PES,
                     how="inner",
                     left_on=['names_cen', 'EAid_cen'],
                     right_on=['names_pes', 'EAid_pes'])

# CLERICAL Matchkey 3: 
matches_3 = pd.merge(left=CEN,
                     right=PES,
                     how="inner",
                     left_on=['names_cen', 'EAid_cen'],
                     right_on=['names_pes', 'EAid_pes'])

# List of matchkey results
matches_list = [matches_1, matches_2, matches_3]

# Empty DataFrame
df = pd.DataFrame()

# Combine results, assign matchkey number and deduplicate
for i, matches in enumerate(matches_list):
    # Next matchkey to add + MK number
    matches['MK'] = i + 1

    # Combine        
    df = pd.concat([df, matches])

    # Identify the lowest MK number for exact duplicates
    df['Min_MK'] = df.groupby(['puid_cen', 'puid_pes'])['MK'].transform('min')

    # Deduplicate (e.g. A-B on MK1 and A-B on MK2)
    df = df[df.Min_MK == df.MK]

    # Move onto next matchkey

# Send all candidates to clerical review
df['CLERICAL'] = 1
df.to_csv(CHECKPOINT_PATH + 'Stage_3_Clerical_MK_EA_Checkpoint.csv', header=True, index=False)
CROW_records = df.copy()

# Add cluster number to records
CROW_records = cluster_number(CROW_records, id_column='puid', suffix_1="_cen", suffix_2="_pes")  # Add cluster ID

# Save records for clerical in the correct format for CROW
CROW_variables = ['puid', 'hid', 'names', 'dob', 'month', 'year', 'relationship', 'sex', 'marstat']
CROW_records_1 = CROW_records[[var + "_cen" for var in CROW_variables] + ['Cluster_Number']].drop_duplicates()
CROW_records_2 = CROW_records[[var + "_pes" for var in CROW_variables] + ['Cluster_Number']].drop_duplicates()
CROW_records_1.columns = CROW_records_1.columns.str.replace(r'_cen$', '', regex=True)
CROW_records_2.columns = CROW_records_2.columns.str.replace(r'_pes$', '', regex=True)
CROW_records_1['Source_Dataset'] = 'cen'  # Dataset indicator
CROW_records_2['Source_Dataset'] = 'pes'  # Dataset indicator
CROW_records_final = pd.concat([CROW_records_1, CROW_records_2], axis=0).sort_values(
    ['Cluster_Number'])  # Combine two datasets together

CROW_records_final.to_csv(CLERICAL_PATH + 'Stage_3_Clerical_MK_EA_Candidates.csv', header=True, index=False)  # Save ready for CROW
