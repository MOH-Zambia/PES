# Import any packages required
import pandas as pd
import numpy as np
import functools
import re
import sys
sys.path.insert(0, "../")
from lib.PARAMETERS import *

# Read in the census data
CEN = pd.read_csv(DATA_PATH + 'census_cleaned.csv', index_col=False)
print("Census read in")

# Read in the PES data
PES = pd.read_csv(DATA_PATH + 'pes_cleaned.csv', index_col=False)
print("PES read in")

# -------------------------------------------------------------------- #
# ----------------- ESTIMATING FALSE NEGATIVE RATE  ------------------ #
# -------------------------------------------------------------------- #    

# Sample of 100 unmatched PES records
unmatched_PES = pd.read_csv(DATA_PATH + 'Stage_6_Unmatched_PES_Sample.csv')  # 100

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
variables = ['puid', 'hid', 'EAid', 'DSid', 'names', 'year', 'month', 'sex', 'relationship']
pes_variables = [x + '_pes' for x in variables]
cen_variables = [x + '_cen' for x in variables]

# View unmatched PES record 
target_record = PES[PES.puid_pes == '111'][pes_variables]
pd.melt(target_record)

"""
5) View all other records from the PES household
"""

# View all other records from the household (matched or unmatched)
target_household_ID = target_record['hid_pes'][0]
target_household = PES[PES.hid_pes == target_household_ID][pes_variables]

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

c1 = CEN.forename_cen == 'CHARLIE'
c2 = CEN.last_name_cen == 'T'
c3 = CEN.year_cen.between(1990, 1995)
c4 = CEN.month_cen.between(5, 12)
c5 = CEN.sex_cen == '1'
c6 = CEN.EAid_cen == "123"

# Apply chosen filters to census dataset
conditions_list = [c1, c2, c3, c4, c5, c6]
census_candidates = CEN[conjunction(*conditions_list)][cen_variables]
print("Search has produced {} potential census match/matches".format(len(census_candidates)))

"""
7) If you think you may have found a census record that matches to the PES record, you may want to view the other 
    records in the census household. "census_household" can be viewed in the variable explorer
"""

# If you want to view the full household for a census puid/record, use this code
census_household_ID = CEN[CEN.puid_cen == '111']['hid_cen'][0]
census_household = CEN[CEN.hid_cen == census_household_ID][cen_variables]

# -------------------------------------------------------------------------- #
# -------------------------------------------------------------------------- #
# -------------------------------------------------------------------------- #


"""
8) Other filters that you may want to try in step 6 - just add them to "conditions_list" ([c1,c2,c3.....cN])
"""

# N-grams e.g. first 2 letters of first name / last 5 letters of last name
CEN.forename_cen.str[0:2] == 'CH'
CEN.forename_cen.str[-5:] == 'ARLIE'

# Missing value filter
CEN.last_name_cen.isnull()

# Filter multiple possible first names
CEN.forename_cen.isin(['CHARLIE', 'CHARLES', 'CHAZ'])


# Wildcard - Search for a name and allow for one or more characters where the .+ is
#  e.g. CHARLIE, CHABLIE, CHARLLE, CHRALIE, CHALRIE, CHAARLIE etc.
def wildcard(string):
    if re.search('CH.+IE', string):
        return True
    else:
        return False


# Apply wildcard filter
CEN.forename_cen.apply(wildcard)

# Other variables to filter on:
# Head of Household
# Relationship to Head of Household
# Marital Status
# Different levels of geography e.g. HH, EA, District


PES = pd.DataFrame({'puid_pes': ['111', '222', '333', '444', '555'],
                    'hid_pes': ['11', '11', '11', '22', '22'],
                    'names_pes': ['CHARLIE T ', 'STEVE X', 'JOHN P', 'BOB Y', 'PETE'],
                    'forename_pes': ['CHARLIE', 'STEVE', 'JOHN', 'BOB', 'PETE'],
                    'last_name_pes': ['T', 'X', 'P', 'Y', None],
                    'year_pes': [1993, 1999, 1992, 2000, 1970],
                    'month_pes': [7, 8, 1, 12, 6],
                    'sex_pes': ['1', '2', '2', '2', '1'],
                    'EAid_pes': ['123', '456', '789', '123', '456'],
                    'DSid_pes': ['1', '4', '7', '1', '4'],
                    'relationship_pes': ['1', '2', '2', '1', '3']})

CEN = pd.DataFrame({'puid_cen': ['111', '222', '333', '444', '555'],
                    'hid_cen': ['11', '11', '11', '22', '22'],
                    'names_cen': ['CHARLES T ', 'STEVE X', 'JOHN P', 'BOB Y', None],
                    'forename_cen': ['CHARLES', 'STEVE', 'JOHN', 'BOB', None],
                    'last_name_cen': ['T', 'X', 'P', 'Y', None],
                    'year_cen': [1993, 1999, 1992, 2000, 1970],
                    'month_cen': [7, 8, 1, 12, 6],
                    'sex_cen': ['1', '2', '2', '2', '1'],
                    'EAid_cen': ['123', '456', '789', '123', '456'],
                    'DSid_cen': ['1', '4', '7', '1', '4'],
                    'relationship_cen': ['1', '2', '2', '1', '3']})
