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

df = pd.read_csv(DATA_PATH + 'Stage_2_Within_EA_Checkpoint.csv')

# --------------------------- CLERICAL MATCHING -----------------------------------#


# Open clerical results from CROW
clerical_results = pd.read_csv(DATA_PATH + 'Stage_2_Within_EA_Matchkey_Clerical_DONE.csv')
clerical_results['clerical_match'] = 1

# Join clerical results onto matches
df = df.merge(clerical_results[['puid_cen', 'puid_pes', 'clerical_match']], how="left", on=['puid_cen', 'puid_pes'])

# Filter to keep auto matches (CLERICAL = 0) + accepted clerical matches (clerical_match = 1)
df = df[((df['CLERICAL'] == 0) | (df['clerical_match'] == 1))]

# Match Type Indicator
df['Match_Type'] = "Within_EA_Matchkey"

# Columns to keep
df = df[['puid_cen', 'puid_pes', 'MK', 'Match_Type', 'CLERICAL']]

# ----------------------------------------------------------------- #
# ----------- STAGE 3: WITHIN EA ASSOCIATIVE MATCHING ------------- #
# ----------------------------------------------------------------- #

# CEN residuals - only use census records that have not been matched yet
CEN_R = CEN.merge(df[['puid_cen']], on='puid_cen', how='left', indicator=True)
CEN_R = CEN_R[CEN_R['_merge'] == 'left_only'].drop('_merge', axis=1)

# PES residuals - only use PES records that have not been matched yet
PES_R = PES.merge(df[['puid_pes']], on='puid_pes', how='left', indicator=True)
PES_R = PES_R[PES_R['_merge'] == 'left_only'].drop('_merge', axis=1)

# Collect HH ID pairs from matches made so far
HH_pairs = df[['hhid_cen', 'hhid_pes']].drop_duplicates()

# Join HH ID pairs onto census/PES residuals (inner join keeps only records where 1+ person from census/PES HH already matched)
CEN_R = CEN_R.merge(HH_pairs, on='hhid_cen', how='inner')
PES_R = PES_R.merge(HH_pairs, on='hhid_pes', how='inner')

# Can now apply rules to match candidates within households already containing 1+ person match
# ('HH_ID_cen', 'HH_ID_pes') <- This part is the associative part of the matchkeys

assoc_matches_1 = pd.merge(left=CEN_R,
                           right=PES_R,
                           how="inner",
                           left_on=['forename_cen', 'dob_cen', 'hhid_cen', 'hhid_pes'],
                           right_on=['forename_pes', 'dob_pes', 'hhid_cen', 'hhid_pes'])

assoc_matches_2 = pd.merge(left=CEN_R,
                           right=PES_R,
                           how="inner",
                           left_on=['forename_cen', 'dob_cen', 'hhid_cen', 'hhid_pes'],
                           right_on=['forename_pes', 'dob_pes', 'hhid_cen', 'hhid_pes'])

assoc_matches_3 = pd.merge(left=CEN_R,
                           right=PES_R,
                           how="inner",
                           left_on=['forename_cen', 'dob_cen', 'hhid_cen', 'hhid_pes'],
                           right_on=['forename_pes', 'dob_pes', 'hhid_cen', 'hhid_pes'])

assoc_matches_4 = pd.merge(left=CEN_R,
                           right=PES_R,
                           how="inner",
                           left_on=['forename_cen', 'dob_cen', 'hhid_cen', 'hhid_pes'],
                           right_on=['forename_pes', 'dob_pes', 'hhid_cen', 'hhid_pes'])

print("Associative matchkeys complete")

# List of matchkey results
assoc_matches_list = [assoc_matches_1, assoc_matches_2, assoc_matches_3, assoc_matches_4]


# Empty DataFrame
df2 = pd.DataFrame()

# Combine results, assign matchkey number and deduplicate
for i, assoc_matches in enumerate(assoc_matches_list):
    # Next matchkey to add + MK number
    assoc_matches['MK'] = i + 1

    # Combine
    df2 = pd.concat([df2, assoc_matches])

    # Identify the lowest MK number for exact duplicates
    df2['Min_MK'] = df2.groupby(['puid_cen', 'puid_pes'])['MK'].transform('min')

    # Deduplicate (e.g. A-B on MK1 and A-B on MK2)
    df2 = df2[df2.Min_MK == df2.MK]

    # Move onto next matchkey


# ------------------------------------------------------------------------ #
# ---------- STAGE 4: RESOLVE ASSOCIATIVE MATCHING CONFLICTS ------------- #
# ------------------------------------------------------------------------ #

# Find CEN or PES IDs matched to multiple records
df2['ID_count_1'] = df2.groupby(['puid_cen'])['puid_pes'].transform('count')
df2['ID_count_2'] = df2.groupby(['puid_pes'])['puid_cen'].transform('count')

# Keep only unique matches (CROW not used here)
df2 = df2[((df2['ID_count_1'] == 1) & (df2['ID_count_2'] == 1))]

# Match Type & Clerical Indicators
df2['Match_Type'] = "Within_EA_Associative"
df2['CLERICAL'] = 0

# Columns to keep
df2 = df2[['puid_cen', 'puid_pes', 'MK', 'Match_Type', 'CLERICAL']]

# Combine all matches together
prev_matches = pd.read_csv(DATA_PATH + 'Stage_1_All_Within_HH_Matches.csv')

df3 = pd.concat([prev_matches, df, df2])

# Save
df3.to_csv(DATA_PATH + 'Stage_2_All_Within_EA_Matches.csv', header=True)
