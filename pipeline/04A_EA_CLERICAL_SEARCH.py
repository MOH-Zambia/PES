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

# ------------------------------------------------------------------------ #
# ----------------- STAGE 1: CLERICAL SEARCH WITHIN EA  ------------------ #
# ------------------------------------------------------------------------ #    

# Read in all matches made so far
prev_matches = pd.read_csv(OUTPUT_PATH + 'Stage_3_Clerical_MK_EA_Matches.csv')

# CEN residuals
CEN = CEN.merge(prev_matches[['puid_cen']], on='puid_cen', how='left', indicator=True)
CEN = CEN[CEN['_merge'] == 'left_only'].drop('_merge', axis=1)

# PES residuals
PES = PES.merge(prev_matches[['puid_pes']], on='puid_pes', how='left', indicator=True)
PES = PES[PES['_merge'] == 'left_only'].drop('_merge', axis=1)

# Collect DataFrame of unique PES EAs
PES_EA_list = PES[['EAid_pes']].drop_duplicates()
PES_EA_list.rename(columns={'EAid_pes': 'EAid_cen'}, inplace=True)

# Filter Census residuals to keep only records from PES_EA list
PES_R = PES.copy()
CEN_R = CEN.merge(PES_EA_list, on="EAid_cen", how='inner')


try:
    os.mkdir(CLERICAL_PATH + "Stage_4/")
except:
    pass
# Loop through each EA
for EA in PES_EA_list.EAid_cen.values.tolist():

    # Filter CEN and PES residuals to keep only residuals from an EA
    CEN_EA = CEN_R[CEN_R.EAid_cen == EA]
    PES_EA = PES_R[PES_R.EAid_pes == EA]

    # Warning if there are no CEN/PES residuals from an EA
    if len(CEN_EA) == 0:
        warnings.warn("No census residuals in EA{}".format(str(EA)))

    if len(PES_EA) == 0:
        warnings.warn("No PES residuals in EA{}".format(str(EA)))

    # Select columns in order you want to save them
    CEN_EA = CEN_EA[['puid_cen', 'names_cen', 'dob_cen', 'month_cen', 'year_cen', 'sex_cen', 'marstatdesc_cen']]
    PES_EA = PES_EA[['puid_pes', 'names_pes', 'dob_pes', 'month_pes', 'year_pes', 'sex_pes', 'marstatdesc_pes']]

    # Save
    CEN_EA.to_csv(CLERICAL_PATH + "Stage_4/" + 'Stage_4_Within_EA_Clerical_Search_CEN_Records_EA{}.csv'.format(str(EA)), header=True, index=False)
    PES_EA.to_csv(CLERICAL_PATH + "Stage_4/" + 'Stage_4_Within_EA_Clerical_Search_PES_Records_EA{}.csv'.format(str(EA)), header=True, index=False)
