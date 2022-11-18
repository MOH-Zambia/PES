# Import any packages required
import pandas as pd
import numpy as np
import jellyfish
import os
import sys

sys.path.insert(0, "../")
from lib.PARAMETERS import *
from lib.CROW_cluster_output_updater import CROW_output_updater

# Read in the census data
CEN = pd.read_csv(DATA_PATH + 'census_cleaned.csv', index_col=False)
print("Census read in")

# Read in the PES data
PES = pd.read_csv(DATA_PATH + 'pes_cleaned.csv', index_col=False)
print("PES read in")

df = pd.read_csv(CHECKPOINT_PATH + 'Stage_3_Clerical_MK_EA_Checkpoint.csv')

# --------------------------- CLERICAL MATCHING -----------------------------------#

# Open clerical results from CROW
clerical_results = pd.read_csv(CLERICAL_PATH + 'Stage_3_Clerical_MK_EA_Candidates_DONE.csv')
clerical_results = CROW_output_updater(output_df=clerical_results, ID_column='puid', Source_column='Source_Dataset',
                                       df1_name='cen', df2_name='pes')
clerical_results.to_csv(CLERICAL_PATH + 'Stage_3_Clerical_MK_EA_Candidates_Reformatted.csv', index=False)
clerical_results['clerical_match'] = 1

# Join clerical results onto matches
df = df.merge(clerical_results[['puid_cen', 'puid_pes', 'clerical_match']], how="left", on=['puid_cen', 'puid_pes'])

# Filter to keep auto matches (CLERICAL = 0) + accepted clerical matches (clerical_match = 1)
df = df[((df['CLERICAL'] == 0) | (df['clerical_match'] == 1))]

# Match Type Indicator
df['Match_Type'] = "Within_EA_Clerical_MK"

# Columns to keep
df = df[['puid_cen', 'puid_pes', 'hid_cen', 'hid_pes', 'MK', 'Match_Type', 'CLERICAL']]

# Combine all matches together
prev_matches = pd.read_csv(OUTPUT_PATH + 'Stage_2_All_Within_EA_Matches.csv')
df2 = pd.concat([prev_matches, df])

# Save
df2.to_csv(OUTPUT_PATH + 'Stage_3_Clerical_MK_EA_Matches.csv', header=True, index=False)
