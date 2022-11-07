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

# ----------------------------------------------------------------- #
# ----------------- STAGE 1: MATCHING WITHIN EA  ------------------ #
# ----------------------------------------------------------------- #    

# Read in all matches made so far
prev_matches = pd.read_csv(DATA_PATH + 'Stage_1_All_Within_HH_Matches.csv')

# CEN residuals
CEN = CEN.merge(prev_matches[['puid_cen']], on='puid_cen', how='left', indicator=True)
CEN = CEN[CEN['_merge'] == 'left_only'].drop('_merge', axis=1)

# PES residuals
PES = PES.merge(prev_matches[['puid_pes']], on='puid_pes', how='left', indicator=True)
PES = PES[PES['_merge'] == 'left_only'].drop('_merge', axis=1)

# Matchkey 1: Full Name + Year + Month + EA
matches_1 = pd.merge(left=CEN,
                     right=PES,
                     how="inner",
                     left_on=['names_cen', 'year_birth_cen', 'birth_month_cen', 'EAid_cen'],
                     right_on=['names_pes', 'year_birth_pes', 'birth_month_pes', 'EAid_pes'])

# Matchkey 2: Edit Distance < 2 + Year + Month + EA          
matches_2 = pd.merge(left=CEN,
                     right=PES,
                     how="inner",
                     left_on=['year_birth_cen', 'birth_month_cen', 'EAid_cen'],
                     right_on=['year_birth_pes', 'birth_month_pes', 'EAid_pes'])
matches_2['EDIT'] = matches_2[['names_cen', 'names_pes']].apply(
    lambda x: jellyfish.levenshtein_distance(str(x[0]), str(x[1])), axis=1)
matches_2 = matches_2[matches_2.EDIT < 2]

# Matchkey 3: Full Name + Year + Sex + EA
matches_3 = pd.merge(left=CEN,
                     right=PES,
                     how="inner",
                     left_on=['names_cen', 'year_birth_cen', 'sex_cen', 'EAid_cen'],
                     right_on=['names_pes', 'year_birth_pes', 'sex_pes', 'EAid_pes'])

# Matchkey 4: Full Name + Age + Sex + EA
matches_4 = pd.merge(left=CEN,
                     right=PES,
                     how="inner",
                     left_on=['names_cen', 'age_cen', 'sex_cen', 'EAid_cen'],
                     right_on=['names_pes', 'age_pes', 'sex_pes', 'EAid_pes'])

# # Matchkey 5: Allowing age/year/month to be different
matches_5 = pd.merge(left=CEN,
                     right=PES,
                     how="inner",
                     left_on=['names_cen', 'sex_cen', 'relationship_hh_cen', 'EAid_cen'],
                     right_on=['names_pes', 'sex_pes', 'relationship_hh_pes', 'EAid_pes'])

# Matchkey 6: Allowing last name to be different
matches_6 = pd.merge(left=CEN,
                     right=PES,
                     how="inner",
                     left_on=['first_name_cen', 'year_birth_cen', 'sex_cen', 'relationship_hh_cen', 'EAid_cen'],
                     right_on=['first_name_pes', 'year_birth_pes', 'sex_pes', 'relationship_hh_pes', 'EAid_pes'])

# Matchkey 7: Allowing first name to be different
matches_7 = pd.merge(left=CEN,
                     right=PES,
                     how="inner",
                     left_on=['last_name_cen', 'year_birth_cen', 'sex_cen', 'relationship_hh_cen', 'EAid_cen'],
                     right_on=['last_name_pes', 'year_birth_pes', 'sex_pes', 'relationship_hh_pes', 'EAid_pes'])

# Matchkey 8: Swapped names
matches_8 = pd.merge(left=CEN,
                     right=PES,
                     how="inner",
                     left_on=['last_name_cen', 'first_name_cen', 'year_birth_cen', 'sex_cen', 'relationship_hh_cen',
                              'EAid_cen'],
                     right_on=['first_name_pes', 'last_name_pes', 'year_birth_pes', 'sex_pes', 'relationship_hh_pes',
                               'EAid_pes'])

# Matchkey 9: Swapped names with a small amount of error 
matches_9 = pd.merge(left=CEN,
                     right=PES,
                     how="inner",
                     left_on=['year_birth_cen', 'sex_cen', 'relationship_hh_cen', 'EAid_cen'],
                     right_on=['year_birth_pes', 'sex_pes', 'relationship_hh_pes', 'EAid_pes'])
matches_9['EDIT1'] = matches_9[['first_name_pes', 'last_name_cen']].apply(
    lambda x: jellyfish.levenshtein_distance(str(x[0]), str(x[1])), axis=1)
matches_9['EDIT2'] = matches_9[['last_name_pes', 'first_name_cen']].apply(
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
    df = df.append(matches)

    # Identify the lowest MK number for exact duplicates
    df['Min_MK'] = df.groupby(['puid_cen', 'puid_pes'])['MK'].transform('min')

    # Deduplicate (e.g. A-B on MK1 and A-B on MK2)
    df = df[df.Min_MK == df.MK]

    # Move onto next matchkey


# ----------------------------------------------------------------- #
# ------------ STAGE 2: RESOLVE WITHIN EA CONFLICTS --------------- #
# ----------------------------------------------------------------- #        

# Find CEN or PES IDs matched to multiple records
df['ID_count_1'] = df.groupby(['puid_cen'])['puid_pes'].transform('count')
df['ID_count_2'] = df.groupby(['puid_pes'])['puid_cen'].transform('count')

# Clerical resolution indicator for conflicts
# "If either of the counts are greater than 1, then send records to CROW"
df['CLERICAL'] = np.where(((df['ID_count_1'] > 1) | (df['ID_count_2'] > 1)), 1, 0)
df.to_csv(DATA_PATH + 'Stage_2_Within_EA_Checkpoint.csv', header=True)

# Filter records for clerical
CROW_records = df[df['CLERICAL'] == 1]

# Save records for clerical in the correct format for CROW
CROW_records = cluster_number(CROW_records, 'puid_cen', 'puid_pes')  # Add cluster ID
CROW_records_1 = CROW_records[
    ['puid_cen', 'hhid_cen', 'names_cen', 'birth_month_cen', 'year_birth_cen', 'relationship_hh_cen', 'sex_cen',
     'marital_status_cen', 'Cluster_ID']].drop_duplicates()  # Select columns
CROW_records_2 = CROW_records[
    ['puid_pes', 'hhid_pes', 'names_pes', 'birth_month_pes', 'year_birth_pes', 'relationship_hh_pes', 'sex_pes',
     'marital_status_pes', 'Cluster_ID']].drop_duplicates()  # Select columns
CROW_records_1.columns = CROW_records_1.columns.str.replace(r'_cen$', '')
CROW_records_2.columns = CROW_records_2.columns.str.replace(r'_pes$', '')
CROW_records_1.rename(columns={'Record_ID': 'puid'}, inplace=True)  # Rename ID column
CROW_records_2.rename(columns={'Record_ID': 'puid'}, inplace=True)  # Rename ID column
CROW_records_1['Source_Dataset'] = 'cen'  # Dataset indicator
CROW_records_2['Source_Dataset'] = 'pes'  # Dataset indicator
CROW_records_final = pd.concat([CROW_records_1, CROW_records_2], axis=0).sort_values(
    ['Cluster_ID'])  # Combine two datasets together
CROW_records_final.to_csv(DATA_PATH + 'Stage_2_Within_EA_Matchkey_Clerical.csv', header=True)  # Save ready for CROW