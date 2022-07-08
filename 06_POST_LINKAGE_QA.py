# Import any packages required
import pandas as pd
import numpy as np
import os
import functools
import re
os.chdir("C:/Users/Rachel/Documents")

# Read in the census data
CEN = pd.read_csv('census_cleaned.csv', index_col=False)
print("Census read in") 

# Read in the PES data
PES = pd.read_csv('pes_cleaned.csv', index_col=False)  
print("PES read in")

# -------------------------------------------------------------------- #
# ----------------- ESTIMATING FALSE NEGATIVE RATE  ------------------ #
# -------------------------------------------------------------------- #    

# Sample of 100 unmatched PES records
unmatched_PES = pd.read_csv('Stage_6_Unmatched_PES_Sample.csv') # 100

"""
HOW TO RUN CLERICAL SEARCH USING UNMATCHED PES SAMPLE:
    
1) Open csv above (Stage_6_Unmatched_PES_Sample) in Excel and keep open whilst matching
2) Create new column 'MATCH' (in Excel) which will contain a 1 if a match is found in the census, otherwise 0
3) Create new column 'CENSUS_ID' (in Excel) which will contain the census puid if a match is found, otherwise 0
"""

"""
4) Take an unmatched PES record from the sample and view all variables of interest
"""

# Variables of interest
variables = ['puid', 'hhid', 'EAid', 'DSid', 'names', 'year_birth', 'birth_month', 'sex', 'relationship_hh']
pes_variables = [x + '_pes' for x in variables]
cen_variables = [x + '_cen' for x in variables]

# View unmatched PES record 
target_record = PES[PES.puid_pes == '999999999999999'][pes_variables]
pd.melt(target_record)

"""
5) View all other records from the PES household
"""

# View all other records from the household (matched or unmatched)
target_household_ID = target_record['hhid_pes'][0]
target_household = PES[PES.hhid_pes == target_household_ID][pes_variables]

"""
6) Search for a match in the full census dataset using a selection of different filters
   Comment out filters not used and add in any extras if you find they work better at finding matches
   Potential census matches ("census_candidates") can be viewed in the variable explorer
"""
    
# Function to combine and apply multiple filters to the census dataset
def conjunction(*conditions):
    return functools.reduce(np.logical_and, conditions)

# ------------------------------------------------ #
# ------ ADD LIST OF CENSUS FILTERS BELOW  ------- #
# ------------------------------------------------ #  

c1 = CEN.first_name_cen == 'CHARLIE'
c2 = CEN.last_name_cen == 'CHARLIE'
c3 = CEN.year_birth_cen.between(1990, 1995)
c4 = CEN.birth_month_cen.between(5, 12)
c5 = CEN.sex_cen == '1'
c6 = CEN.EAid_cen == "123"

# Apply chosen filters to census dataset
conditions_list = [c1, c2, c3, c4, c5, c6]
census_candidates = CEN[conjunction(*conditions_list)]
print("Search has produced {} potential census match/matches".format(len(census_candidates)))

"""
7) If you think you may have found a census record that matches to the PES record, you may want to view the other 
    records in the census household. "census_household" can be viewed in the variable explorer
"""

# If you want to view the full household for a census puid/record, use this code
census_household_ID = CEN[CEN.puid_cen == '111']['hhid_cen'][0]
census_household = CEN[CEN.hhid_cen == census_household_ID][cen_variables]

# -------------------------------------------------------------------------- #

"""
8) Other filters that you may want to try in step 6 - just add them to "conditions_list" ([c1,c2,c3.....cN])
"""

# N-grams e.g. first 2 letters of first name / last 5 letters of last name
CEN.first_name_cen.str[0:2] == 'CH'
CEN.last_name_cen.str[-5:] == 'ARLIE'

# Missing value filter
CEN.last_name_cen.isnull()

# Filter multiple possible first names
CEN.first_name_cen.isin(['CHARLIE', 'CHARLES', 'CHAZ'])

# Wildard - Search for a name and allow for one or more characters where the .+ is
#  e.g. CHARLIE, CHABLIE, CHARLLE, CHRALIE, CHALRIE, CHAARLIE etc.
def wildcard(string):
    if re.search('CH.+IE',string): return True
    else: return False
  
# Apply wildcard filter
CEN.first_name_cen.apply(wildcard)



  
























CEN = pd.DataFrame({'puid_cen': ['111', '222', '333', '444', '555'],
                    'hhid_cen': ['11', '11', '11', '22', '22'],
                    'first_name_cen': ['CHARLIE', 'STEVE', 'JOHN', 'BOB', 'PETE'],
                    'last_name_cen': ['CHARLIE', 'STEVE', 'JOHN', 'BOB', None],
                    'year_birth_cen': [1993, 1999, 1992, 2000, 1970],
                    'birth_month_cen': [7, 8, 1, 12, 6],
                    'sex_cen': ['1', '2', '2', '2', '1'],
                    'EAid_cen': ['123', '456', '789', '123', '456']})