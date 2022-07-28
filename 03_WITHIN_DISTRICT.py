# Import any packages required
import pandas as pd
import numpy as np
import networkx as nx
import jellyfish
import os

# Cluster Function
os.chdir("C:/Users/Rachel/Documents")
from Cluster_Function import cluster_number

# Read in the census data
CEN = pd.read_csv('census_cleaned.csv', index_col=False)
print("Census read in") 

# Read in the PES data
PES = pd.read_csv('pes_cleaned.csv', index_col=False)  
print("PES read in")

# ----------------------------------------------------------------------- #
# ----------------- STAGE 1: MATCHING WITHIN DISTRICT  ------------------ #
# ----------------------------------------------------------------------- #    

# Read in all matches made so far
prev_matches = pd.read_csv('Stage_2_All_Within_EA_Matches.csv')

# CEN residuals
CEN = CEN.merge(prev_matches[['puid_cen']], on = 'puid_cen', how = 'left', indicator = True)
CEN = CEN[CEN['_merge'] == 'left_only'].drop('_merge', axis=1)

# PES residuals
PES = PES.merge(prev_matches[['puid_pes']], on = 'puid_pes', how = 'left', indicator = True)
PES = PES[PES['_merge'] == 'left_only'].drop('_merge', axis=1)

# Matchkey 1: Full Name + Year + Month + District
matches_1 = pd.merge(left=CEN,
                     right=PES,
                     how="inner",
                     left_on =['names_cen', 'year_birth_cen', 'birth_month_cen', 'DSid_cen'],
                     right_on=['names_pes', 'year_birth_pes', 'birth_month_pes', 'DSid_pes'])

# Matchkey 2: Edit Distance < 2 + Year + Month + District          
matches_2 = pd.merge(left=CEN,
                     right=PES,
                     how="inner",
                     left_on =['year_birth_cen', 'birth_month_cen', 'DSid_cen'],
                     right_on=['year_birth_pes', 'birth_month_pes', 'DSid_pes'])
matches_2['EDIT'] = matches_2[['names_cen','names_pes']].apply(lambda x: jellyfish.levenshtein_distance(str(x[0]), str(x[1])), axis=1)
matches_2 = matches_2[matches_2.EDIT < 2]


#
# Add extra MKs here within district
#


# List of matchkey results
matches_list = [matches_1, matches_2]

# Start with results from MK1 (i=0)
i = 0

# Empty DataFrame
df = pd.DataFrame()

# Combine results, assign matchkey number and deduplicate
for matches in matches_list:
    
    # Next matchkey to add + MK number
    matches = matches_list[i]
    matches['MK'] = i+1

    # Combine        
    df = df.append(matches)

    # Identify lowest MK number for exact duplicates
    df['Min_MK'] = df.groupby(['puid_cen', 'puid_pes'])['MK'].transform('min')
    
    # Deduplicate (e.g. A-B on MK1 and A-B on MK2)
    df = df[df.Min_MK == df.MK]
        
    # Move onto next matchkey
    i+=1

# ----------------------------------------------------------------------- #
# ------------ STAGE 2: RESOLVE WITHIN DISTRICT CONFLICTS --------------- #
# ----------------------------------------------------------------------- #        

# Find CEN or PES IDs matched to mutliple records
df['ID_count_1'] = df.groupby(['puid_cen'])['puid_pes'].transform('count')
df['ID_count_2'] = df.groupby(['puid_pes'])['puid_cen'].transform('count')

# Clerical resolution indicator for conflicts
# "If either of the counts are greater than 1, then send records to CROW"
df['CLERICAL'] = np.where(((df['ID_count_1'] > 1) | (df['ID_count_2'] > 1)), 1,0)

# Filter records for clerical
CROW_records = df[df['CLERICAL'] == 1]

# Save records for clerical in the correct format for CROW
CROW_records = cluster_number(CROW_records, 'puid_cen', 'puid_pes') # Add cluster ID
CROW_records_1 = CROW_records[['puid_cen', 'hhid_cen', 'names_cen', 'birth_month_cen', 'year_birth_cen', 'relationship_hh_cen', 'sex_cen', 'marital_status_cen', 'Cluster_ID']].drop_duplicates() # Select columns
CROW_records_2 = CROW_records[['puid_pes', 'hhid_pes', 'names_pes', 'birth_month_pes', 'year_birth_pes', 'relationship_hh_pes', 'sex_pes', 'marital_status_pes', 'Cluster_ID']].drop_duplicates() # Select columns
CROW_records_1.columns = CROW_records_1.columns.str.rstrip('_cen')  # Remove suffixes
CROW_records_2.columns = CROW_records_2.columns.str.rstrip('_pes')  # Remove suffixes
CROW_records_1.rename(columns = {'Record_ID' : 'puid'}, inplace = True) # Rename ID column
CROW_records_2.rename(columns = {'Record_ID' : 'puid'}, inplace = True) # Rename ID column
CROW_records_1['Source_Dataset'] = 'cen' # Dataset indicator
CROW_records_2['Source_Dataset'] = 'pes' # Dataset indicator
CROW_records_final = pd.concat([CROW_records_1, CROW_records_2], axis=0).sort_values(['Cluster_ID']) # Combine two dataets together
CROW_records_final.to_csv('Stage_3_Within_DS_Matchkey_Clerical.csv', header = True) # Save ready for CROW


#--------------------------- CLERICAL MATCHING -----------------------------------#


# Open clerical results from CROW
clerical_results = pd.read_csv('Stage_3_Within_DS_Matchkey_Clerical_DONE.csv') 
clerical_results['clerical_match'] = 1

# Join clerical results onto matches
df = df.merge(clerical_results[['puid_cen', 'puid_pes', 'clerical_match']], how="left", on =['puid_cen', 'puid_pes'])

# Filter to keep auto matches (CLERICAL = 0) + accepted clerical matches (clerical_match = 1)
df = df[((df['CLERICAL'] == 0) | (df['clerical_match'] == 1))]

# Match Type Indicator
df['Match_Type'] = "Within_DS_Matchkey"

# Columns to keep
df = df[['puid_cen', 'puid_pes', 'MK', 'Match_Type', 'CLERICAL']]

# ----------------------------------------------------------------------- #
# ----------- STAGE 3: WITHIN DISTRICT ASSOCIATIVE MATCHING ------------- #
# ----------------------------------------------------------------------- #  

# CEN residuals - only use census records that have not been matched yet
CEN_R = CEN.merge(df[['puid_cen']], on = 'puid_cen', how = 'left', indicator = True)
CEN_R = CEN_R[CEN_R['_merge'] == 'left_only'].drop('_merge', axis=1)

# PES residuals - only use PES records that have not been matched yet
PES_R = PES.merge(df[['puid_pes']], on = 'puid_pes', how = 'left', indicator = True)
PES_R = PES_R[PES_R['_merge'] == 'left_only'].drop('_merge', axis=1)

# Collect HH ID pairs from matches made so far
HH_pairs = df[['hhid_cen', 'hhid_pes']].drop_duplicates()

# Join HH ID pairs onto census/PES residuals (inner join keeps only records where 1+ person from census/PES HH already matched)
CEN_R = CEN_R.merge(HH_pairs, on = 'hhid_cen', how = 'inner')
PES_R = PES_R.merge(HH_pairs, on = 'hhid_pes', how = 'inner')

# Can now apply rules to match candidates within households already containing 1+ person match
# ('HH_ID_cen', 'HH_ID_pes') <- This part is the associative part of the matchkeys

assoc_matches_1 = pd.merge(left=CEN_R,
                     right=PES_R,
                     how="inner",
                     left_on =['forename_cen', 'dob_cen', 'hhid_cen', 'hhid_pes'],
                     right_on=['forename_pes', 'dob_pes', 'hhid_cen', 'hhid_pes'])
          
assoc_matches_2 = pd.merge(left=CEN_R,
                     right=PES_R,
                     how="inner",
                     left_on =['forename_cen', 'dob_cen', 'hhid_cen', 'hhid_pes'],
                     right_on=['forename_pes', 'dob_pes', 'hhid_cen', 'hhid_pes'])
          
assoc_matches_3 = pd.merge(left=CEN_R,
                     right=PES_R,
                     how="inner",
                     left_on =['forename_cen', 'dob_cen', 'hhid_cen', 'hhid_pes'],
                     right_on=['forename_pes', 'dob_pes', 'hhid_cen', 'hhid_pes'])

assoc_matches_4 = pd.merge(left=CEN_R,
                     right=PES_R,
                     how="inner",
                     left_on =['forename_cen', 'dob_cen', 'hhid_cen', 'hhid_pes'],
                     right_on=['forename_pes', 'dob_pes', 'hhid_cen', 'hhid_pes'])

print("Associative matchkeys complete")

# List of matchkey results
assoc_matches_list = [assoc_matches_1,assoc_matches_2,assoc_matches_3,assoc_matches_4]

# Start with results from MK1 (i=0)
i = 0

# Empty DataFrame
df2 = pd.DataFrame()

# Combine results, assign matchkey number and deduplicate
for assoc_matches in assoc_matches_list:
    
    # Next matchkey to add + MK number
    assoc_matches = assoc_matches_list[i]
    assoc_matches['MK'] = i+1

    # Combine        
    df2 = df2.append(assoc_matches)
    
    # Identify lowest MK number for exact duplicates
    df2['Min_MK'] = df2.groupby(['puid_cen', 'puid_pes'])['MK'].transform('min')
    
    # Deduplicate (e.g. A-B on MK1 and A-B on MK2)
    df2 = df2[df2.Min_MK == df2.MK]
    
    # Move onto next matchkey
    i+=1
    
# ------------------------------------------------------------------------ #
# ---------- STAGE 4: RESOLVE ASSOCIATIVE MATCHING CONFLICTS ------------- #
# ------------------------------------------------------------------------ #        

# Find CEN or PES IDs matched to mutliple records
df2['ID_count_1'] = df2.groupby(['puid_cen'])['puid_pes'].transform('count')
df2['ID_count_2'] = df2.groupby(['puid_pes'])['puid_cen'].transform('count')

# Keep only unique matches (CROW not used here)
df2 = df2[((df2['ID_count_1'] == 1) & (df2['ID_count_2'] == 1))]

# Match Type & Clerical Indicators
df2['Match_Type'] = "Within_DS_Associative"
df2['CLERICAL'] = 0

# Columns to keep
df2 = df2[['puid_cen', 'puid_pes', 'MK', 'Match_Type', 'CLERICAL']]

# Combine all matches together
df3 = pd.concat([prev_matches,df,df2])

# Save
df3.to_csv('Stage_3_All_Within_DS_Matches.csv', header = True)
