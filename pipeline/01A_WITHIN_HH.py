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

# Empty DataFrame
df = pd.DataFrame()

# Combine results, assign matchkey number and deduplicate
for i, matches in enumerate(matches_list):
    # Next matchkey to add + MK number
    matches['MK'] = i + 1

    # Combine        
    df = pd.concat([df, matches])

    # Identify the lowest MK number for exact duplicates
    df['Min_MK'] = df.groupby(['id_indi_cen', 'id_indi_pes'])['MK'].transform('min')

    # Deduplicate (e.g. A-B on MK1 and A-B on MK2)
    df = df[df.Min_MK == df.MK]

    # Move onto next matchkey

# ------------------------------------------------------------------------ #
# ------------ STAGE 2: RESOLVE WITHIN HOUSEHOLD CONFLICTS --------------- #
# ------------------------------------------------------------------------ #        

# Find CEN or PES IDs matched to multiple records
df['ID_count_1'] = df.groupby(['id_indi_cen'])['id_indi_pes'].transform('count')
df['ID_count_2'] = df.groupby(['id_indi_pes'])['id_indi_cen'].transform('count')

# Clerical resolution indicator for conflicts
# "If either of the counts are greater than 1, then send records to CROW"
df['CLERICAL'] = np.where(((df['ID_count_1'] > 1) | (df['ID_count_2'] > 1)), 1, 0)

# Save a checkpoint file for the stage after clerical
df.to_csv(DATA_PATH + 'Stage_1_Within_HH_Checkpoint.csv', header=True)
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
CROW_records_1.columns = CROW_records_1.columns.str.replace(r'_cen$', '', regex=True)
CROW_records_2.columns = CROW_records_2.columns.str.replace(r'_pes$', '', regex=True)
CROW_records_1.rename(columns={'Record_ID': 'id_indi'}, inplace=True)  # Rename ID column
CROW_records_2.rename(columns={'Record_ID': 'id_indi'}, inplace=True)  # Rename ID column
CROW_records_1['Source_Dataset'] = 'cen'  # Dataset indicator
CROW_records_2['Source_Dataset'] = 'pes'  # Dataset indicator
CROW_records_final = pd.concat([CROW_records_1, CROW_records_2], axis=0).sort_values(
    ['Cluster_ID'])  # Combine two dataets together
CROW_records_final.to_csv(DATA_PATH + 'Stage_1_Within_HH_Matchkey_Clerical.csv', header=True)  # Save ready for CROW
print("Stage 1 Within HH Preclerical completed.")
