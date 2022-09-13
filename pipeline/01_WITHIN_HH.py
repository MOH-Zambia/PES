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

# ------------------------------------------------------------------------ #
# ----------------- STAGE 1: MATCHING WITHIN HOUSEHOLD  ------------------ #
# ------------------------------------------------------------------------ #    

# Matchkey 1: Full Name + Year + Month + Household
matches_1 = pd.merge(left=CEN,
                     right=PES,
                     how="inner",
                     left_on=['fullnm_cen', 'year_cen', 'month_cen', 'hid_cen'],
                     right_on=['fullnm_pes', 'year_pes', 'month_pes', 'hid_pes'])

# Matchkey 2: Edit Distance < 2 + Year + Month + Household          
matches_2 = pd.merge(left=CEN,
                     right=PES,
                     how="inner",
                     left_on=['year_cen', 'month_cen', 'hid_cen'],
                     right_on=['year_pes', 'month_pes', 'hid_pes'])
matches_2['EDIT'] = matches_2[['fullnm_cen', 'fullnm_pes']].apply(
    lambda x: jellyfish.levenshtein_distance(str(x[0]), str(x[1])), axis=1)
matches_2 = matches_2[matches_2.EDIT < 2]

# Matchkey 3: Full Name + Year + Sex + Household
matches_3 = pd.merge(left=CEN,
                     right=PES,
                     how="inner",
                     left_on=['fullnm_cen', 'year_cen', 'sex_cen', 'hid_cen'],
                     right_on=['fullnm_pes', 'year_pes', 'sex_pes', 'hid_pes'])

# Matchkey 4: Full Name + Age + Sex + Household
matches_4 = pd.merge(left=CEN,
                     right=PES,
                     how="inner",
                     left_on=['fullnm_cen', 'age_cen', 'sex_cen', 'hid_cen'],
                     right_on=['fullnm_pes', 'age_pes', 'sex_pes', 'hid_pes'])

# # Matchkey 5: Allowing age/year/month to be different
matches_5 = pd.merge(left=CEN,
                     right=PES,
                     how="inner",
                     left_on=['fullnm_cen', 'sex_cen', 'relationship_cen', 'hid_cen'],
                     right_on=['fullnm_pes', 'sex_pes', 'relationship_pes', 'hid_pes'])

# Matchkey 6: Allowing last name to be different
matches_6 = pd.merge(left=CEN,
                     right=PES,
                     how="inner",
                     left_on=['firstnm_cen', 'year_cen', 'sex_cen', 'relationship_cen', 'hid_cen'],
                     right_on=['firstnm_pes', 'year_pes', 'sex_pes', 'relationship_pes', 'hid_pes'])

# Matchkey 7: Allowing first name to be different
matches_7 = pd.merge(left=CEN,
                     right=PES,
                     how="inner",
                     left_on=['lastnm_cen', 'year_cen', 'sex_cen', 'relationship_cen', 'hid_cen'],
                     right_on=['lastnm_pes', 'year_pes', 'sex_pes', 'relationship_pes', 'hid_pes'])

# Matchkey 8: Swapped names
matches_8 = pd.merge(left=CEN,
                     right=PES,
                     how="inner",
                     left_on=['lastnm_cen', 'firstnm_cen', 'year_cen', 'sex_cen', 'relationship_cen',
                              'hid_cen'],
                     right_on=['firstnm_pes', 'lastnm_pes', 'year_pes', 'sex_pes', 'relationship_pes',
                               'hid_pes'])

# Matchkey 9: Swapped names with a small amount of error 
matches_9 = pd.merge(left=CEN,
                     right=PES,
                     how="inner",
                     left_on=['year_cen', 'sex_cen', 'relationship_cen', 'hid_cen'],
                     right_on=['year_pes', 'sex_pes', 'relationship_pes', 'hid_pes'])
matches_9['EDIT1'] = matches_9[['firstnm_pes', 'lastnm_cen']].apply(
    lambda x: jellyfish.levenshtein_distance(str(x[0]), str(x[1])), axis=1)
matches_9['EDIT2'] = matches_9[['lastnm_pes', 'firstnm_cen']].apply(
    lambda x: jellyfish.levenshtein_distance(str(x[0]), str(x[1])), axis=1)
matches_9 = matches_9[(matches_9.EDIT1 < 2) | (matches_9.EDIT2 < 2)]

# List of matchkey results
matches_list = [matches_1, matches_2, matches_3, matches_4, matches_5,
                matches_6, matches_7, matches_8, matches_9]

# Start with results from MK1 (i=0)
i = 0

# Empty DataFrame
df = pd.DataFrame()

# Combine results, assign matchkey number and deduplicate
for matches in matches_list:
    # Next matchkey to add + MK number
    matches = matches_list[i]
    matches['MK'] = i + 1

    # Combine        
    df = df.append(matches)

    # Identify lowest MK number for exact duplicates
    df['Min_MK'] = df.groupby(['id_indi_cen', 'id_indi_pes'])['MK'].transform('min')

    # Deduplicate (e.g. A-B on MK1 and A-B on MK2)
    df = df[df.Min_MK == df.MK]

    # Move onto next matchkey
    i += 1

# ------------------------------------------------------------------------ #
# ------------ STAGE 2: RESOLVE WITHIN HOUSEHOLD CONFLICTS --------------- #
# ------------------------------------------------------------------------ #        

# Find CEN or PES IDs matched to multiple records
df['ID_count_1'] = df.groupby(['id_indi_cen'])['id_indi_pes'].transform('count')
df['ID_count_2'] = df.groupby(['id_indi_pes'])['id_indi_cen'].transform('count')

# Clerical resolution indicator for conflicts
# "If either of the counts are greater than 1, then send records to CROW"
df['CLERICAL'] = np.where(((df['ID_count_1'] > 1) | (df['ID_count_2'] > 1)), 1, 0)

# Filter records for clerical
CROW_records = df[df['CLERICAL'] == 1]

# Add cluster number to records
CROW_records = cluster_number(CROW_records, 'id_indi_cen', 'id_indi_pes')  # Add cluster ID

# Use this to create a cluster number if the line above is not working. 
# Note: This will not cluster together non-unique matches; every pair will be sent separately.
# CROW_records['Cluster_ID'] = np.arange(len(CROW_records))

# Save records for clerical in the correct format for CROW
CROW_records_1 = CROW_records[
    ['id_indi_cen', 'hid_cen', 'fullnm_cen', 'month_cen', 'year_cen', 'relationship_cen', 'sex_cen',
     'marstat_cen', 'Cluster_ID']].drop_duplicates()  # Select columns
CROW_records_2 = CROW_records[
    ['id_indi_pes', 'hid_pes', 'fullnm_pes', 'month_pes', 'year_pes', 'relationship_pes', 'sex_pes',
     'marstat_pes', 'Cluster_ID']].drop_duplicates()  # Select columns
CROW_records_1.columns = CROW_records_1.columns.str.replace(r'_cen$', '')
CROW_records_2.columns = CROW_records_2.columns.str.replace(r'_pes$', '')
CROW_records_1.rename(columns={'Record_ID': 'id_indi'}, inplace=True)  # Rename ID column
CROW_records_2.rename(columns={'Record_ID': 'id_indi'}, inplace=True)  # Rename ID column
CROW_records_1['Source_Dataset'] = 'cen'  # Dataset indicator
CROW_records_2['Source_Dataset'] = 'pes'  # Dataset indicator
CROW_records_final = pd.concat([CROW_records_1, CROW_records_2], axis=0).sort_values(
    ['Cluster_ID'])  # Combine two dataets together
CROW_records_final.to_csv(DATA_PATH + 'Stage_1_Within_HH_Matchkey_Clerical.csv', header=True)  # Save ready for CROW

# --------------------------- CLERICAL MATCHING -----------------------------------#


# Open clerical results from CROW
clerical_results = pd.read_csv(DATA_PATH + 'Stage_1_Within_HH_Matchkey_Clerical_DONE.csv')
clerical_results['clerical_match'] = 1

# Join clerical results onto matches
df = df.merge(clerical_results[['id_indi_cen', 'id_indi_pes', 'clerical_match']], how="left", on=['id_indi_cen', 'id_indi_pes'])

# Filter to keep auto matches (CLERICAL = 0) + accepted clerical matches (clerical_match = 1)
df = df[((df['CLERICAL'] == 0) | (df['clerical_match'] == 1))]

# Match Type Indicator
df['Match_Type'] = "Within_HH_Matchkey"

# Columns to keep
df = df[['id_indi_cen', 'id_indi_pes', 'MK', 'Match_Type', 'CLERICAL']]

# ------------------------------------------------------------------------ #
# ----------- STAGE 3: WITHIN HOUSEHOLD ASSOCIATIVE MATCHING ------------- #
# ------------------------------------------------------------------------ #  

# CEN residuals - only use census records that have not been matched yet
CEN_R = CEN.merge(df[['id_indi_cen']], on='id_indi_cen', how='left', indicator=True)
CEN_R = CEN_R[CEN_R['_merge'] == 'left_only'].drop('_merge', axis=1)

# PES residuals - only use PES records that have not been matched yet
PES_R = PES.merge(df[['id_indi_pes']], on='id_indi_pes', how='left', indicator=True)
PES_R = PES_R[PES_R['_merge'] == 'left_only'].drop('_merge', axis=1)

# Collect HH ID pairs from matches made so far
HH_pairs = df[['hid_cen', 'hid_pes']].drop_duplicates()

# Join HH ID pairs onto census/PES residuals (inner join keeps only records where 1+ person from census/PES HH already matched)
CEN_R = CEN_R.merge(HH_pairs, on='hid_cen', how='inner')
PES_R = PES_R.merge(HH_pairs, on='hid_pes', how='inner')

# Can now apply rules to match candidates within households already containing 1+ person match
# ('HH_ID_cen', 'HH_ID_pes') <- This part is the associative part of the matchkeys

assoc_matches_1 = pd.merge(left=CEN_R,
                           right=PES_R,
                           how="inner",
                           left_on=['forename_cen', 'dob_cen', 'hid_cen', 'hid_pes'],
                           right_on=['forename_pes', 'dob_pes', 'hid_cen', 'hid_pes'])

assoc_matches_2 = pd.merge(left=CEN_R,
                           right=PES_R,
                           how="inner",
                           left_on=['forename_cen', 'dob_cen', 'hid_cen', 'hid_pes'],
                           right_on=['forename_pes', 'dob_pes', 'hid_cen', 'hid_pes'])

assoc_matches_3 = pd.merge(left=CEN_R,
                           right=PES_R,
                           how="inner",
                           left_on=['forename_cen', 'dob_cen', 'hid_cen', 'hid_pes'],
                           right_on=['forename_pes', 'dob_pes', 'hid_cen', 'hid_pes'])

assoc_matches_4 = pd.merge(left=CEN_R,
                           right=PES_R,
                           how="inner",
                           left_on=['forename_cen', 'dob_cen', 'hid_cen', 'hid_pes'],
                           right_on=['forename_pes', 'dob_pes', 'hid_cen', 'hid_pes'])

print("Associative matchkeys complete")

# List of matchkey results
assoc_matches_list = [assoc_matches_1, assoc_matches_2, assoc_matches_3, assoc_matches_4]

# Start with results from MK1 (i=0)
i = 0

# Empty DataFrame
df2 = pd.DataFrame()

# Combine results, assign matchkey number and deduplicate
for assoc_matches in assoc_matches_list:
    # Next matchkey to add + MK number
    assoc_matches = assoc_matches_list[i]
    assoc_matches['MK'] = i + 1

    # Combine        
    df2 = df2.append(assoc_matches)

    # Identify lowest MK number for exact duplicates
    df2['Min_MK'] = df2.groupby(['id_indi_cen', 'id_indi_pes'])['MK'].transform('min')

    # Deduplicate (e.g. A-B on MK1 and A-B on MK2)
    df2 = df2[df2.Min_MK == df2.MK]

    # Move onto next matchkey
    i += 1

# ------------------------------------------------------------------------ #
# ---------- STAGE 4: RESOLVE ASSOCIATIVE MATCHING CONFLICTS ------------- #
# ------------------------------------------------------------------------ #        

# Find CEN or PES IDs matched to mutliple records
df2['ID_count_1'] = df2.groupby(['id_indi_cen'])['id_indi_pes'].transform('count')
df2['ID_count_2'] = df2.groupby(['id_indi_pes'])['id_indi_cen'].transform('count')

# Keep only unique matches (CROW not used here)
df2 = df2[((df2['ID_count_1'] == 1) & (df2['ID_count_2'] == 1))]

# Match Type & Clerical Indicators
df2['Match_Type'] = "Within_HH_Associative"
df2['CLERICAL'] = 0

# Columns to keep
df2 = df2[['id_indi_cen', 'id_indi_pes', 'MK', 'Match_Type', 'CLERICAL']]

# Combine all matches together
df3 = pd.concat([df, df2])

# Save
df3.to_csv(DATA_PATH + 'Stage_1_All_Within_HH_Matches.csv', header=True)
