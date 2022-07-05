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

# ------------------------------------------------------------------------ #
# ----------------- STAGE 1: CLERICAL SEARCH WITHIN EA  ------------------ #
# ------------------------------------------------------------------------ #    

# Read in all matches made so far
matches = pd.read_csv('Stage_3_All_Within_DS_Matches.csv')

# CEN residuals
CEN = CEN.merge(matches[['puid_cen']], on = 'puid_cen', how = 'left', indicator = True)
CEN = CEN[CEN['_merge'] == 'left_only'].drop(columns = ['_merge'])

# PES residuals
PES = PES.merge(matches[['puid_pes']], on = 'puid_pes', how = 'left', indicator = True)
PES = PES[PES['_merge'] == 'left_only'].drop(columns = ['_merge'])

# Collect DataFrame of unique PES EAs
PES_EA = PES[['EAid_pes']].drop_duplicates()

# Filter Census residuals to keep only records from PES_EA list
PES_R = PES
CEN_R = CEN.merge(PES_EA, on = [CEN.EAid_cen == PES_EA.EAid_pes], how = 'inner')

# Loop through each EA
for EA in PES_EA.values.tolist():
    
    # Filter CEN and PES residuals to keep only residuals from an EA
    CEN_EA = CEN_R[CEN_R.EAid_cen == EA]
    PES_EA = PES_R[PES_R.EAid_pes == EA]
    
    # Select columns in order you want to save them
    CEN_EA = CEN_EA[['puid_cen', 'names_cen', 'birth_month_cen', 'year_birth_cen', 'sex_cen', 'marital_status_cen']]
    PES_EA = PES_EA[['puid_pes', 'names_pes', 'birth_month_pes', 'year_birth_pes', 'sex_pes', 'marital_status_pes']]
    
    # Save
    CEN_EA.to_csv('Stage_4_Within_EA_Clerical_Search_CEN_Records_EA{}.csv'.format(str(EA)), header = True)
    PES_EA.to_csv('Stage_4_Within_EA_Clerical_Search_PES_Records_EA{}.csv'.format(str(EA)), header = True)
    

#--------------------------- CLERICAL MATCHING IN EXCEL -----------------------------------#


# DataFrame to append results to
all_ea_results = pd.DataFrame()

# Loop through EAs and combine all clerical results from EA 'SNAP'
for EA in PES_EA.values.tolist():
    
    # Read in results from an EA
    ea_results = pd.read_csv('Stage_4_Within_EA_Clerical_Search_EA{}_DONE.csv'.format(str(EA)))
    
    # Take matches only
    ea_results = ea_results[ea_results['clerical_match'] == 1][['puid_cen', 'puid_pes']]
    
    # Combine
    all_ea_results = all_ea_results.append(ea_results)
    
# Match Type Indicator
all_ea_results['Match_Type'] = "Within_EA_Clerical_Search"

# Join other columns back onto 'all_ea_results' before combining all matches
# If other columns are not joined on then the concat below will not work

# Combine above clerical results with all previous matches
df3 = pd.concat(matches,all_ea_results)

# Save
df3.to_csv('Stage_4_All_Clerical_Search_EA_Matches.csv', header = True)
