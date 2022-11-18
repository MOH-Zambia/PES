# Import any packages required
import pandas as pd
import numpy as np
import networkx as nx
import jellyfish
import os
import warnings
import sys
sys.path.insert(0, "../")
from lib.PARAMETERS import *

# Read in the census data
CEN = pd.read_csv(DATA_PATH + 'census_cleaned.csv', index_col=False)
print("Census read in")

# Read in the PES data
PES = pd.read_csv(DATA_PATH + 'pes_cleaned.csv', index_col=False)
print("PES read in")

# Read in all matches made so far
prev_matches = pd.read_csv(OUTPUT_PATH + 'Stage_3_Clerical_MK_EA_Matches.csv')

# --------------------------- CLERICAL MATCHING IN EXCEL -----------------------------------#
# Collect DataFrame of unique PES EAs
PES_EA_list = PES[['EAid_pes']].drop_duplicates()
PES_EA_list.rename(columns={'EAid_pes': 'EAid_cen'}, inplace=True)

# DataFrame to append results to
all_ea_results = pd.DataFrame()

# Loop through EAs and combine all clerical results from EA 'SNAP'
for EA in PES_EA_list.EAid_cen.values.tolist():

    # Only loop through EAs where at least one match was made
    if os.path.exists(CLERICAL_PATH + "Stage_4/" +'Stage_4_Within_EA_Clerical_Search_EA{}_DONE.csv'.format(str(EA))):

        # Read in results from an EA
        ea_results = pd.read_csv(CLERICAL_PATH + "Stage_4/" + 'Stage_4_Within_EA_Clerical_Search_EA{}_DONE.csv'.format(str(EA)))

        # Take columns needed
        ea_results = ea_results[['puid_cen', 'puid_pes']]

        # Combine
        all_ea_results = all_ea_results.append(ea_results)

    else:
        warnings.warn("No clerical search matches made from EA{}".format(str(EA)))


if len(all_ea_results) <1:
    warnings.warn("No clerical search matches made from any EA!")
    all_ea_results[['puid_cen', 'puid_pes']] = None, None


# Add Indicators so that previous matches will concat with new matches
all_ea_results['Match_Type'] = "Within_EA_Clerical_Search"
all_ea_results['CLERICAL'] = 1
all_ea_results['MK'] = 0

# Join on HH IDs before concat
all_ea_results = all_ea_results.merge(CEN[['puid_cen', 'hid_cen']], on='puid_cen', how='left')
all_ea_results = all_ea_results.merge(PES[['puid_pes', 'hid_pes']], on='puid_pes', how='left')

# Columns to keep
all_ea_results = all_ea_results[['puid_cen', 'puid_pes', 'hid_cen', 'hid_pes', 'MK', 'Match_Type', 'CLERICAL']]

# Combine above clerical results with all previous matches
df = pd.concat([prev_matches, all_ea_results])

# Save
df.to_csv(OUTPUT_PATH + 'Stage_4_Clerical_Search_EA_Matches.csv', header=True, index=False)
