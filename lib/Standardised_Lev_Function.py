# Import any packages required
import pandas as pd
import jellyfish
import os
from PARAMETERS import *

# Cluster Function

# Read in the census data
CEN = pd.read_csv(DATA_PATH + 'census_cleaned.csv', index_col=False)
print("Census read in") 

# Read in the PES data
PES = pd.read_csv(DATA_PATH + 'pes_cleaned.csv', index_col=False)  
print("PES read in")

# ------------------------------------------------------------------- #
# ------------- STANDARDISED LEVENSHTEIN EDIT DISTANCE -------------- #
# ------------------------------------------------------------------- # 

# Function that returns a score between 0 and 1
def std_lev(string1, string2):
    
    # Score = None if strings are None
    if string1 is None or string2 is None: 
        return None
    
    # Calculate score if strings are not None
    else:
        # String lengths
        length1, length2 = len(string1), len(string2)
        
        # Max length
        max_length = max(length1, length2)
        
        # Edit Distance
        lev = jellyfish.levenshtein_distance(string1, string2)
        
        # Standardsed Edit Distance
        std_lev = 1 - (lev / max_length)
        
        return std_lev
    
# Test
std_lev('CHARLIE', 'CHARLIE') # 1.00
std_lev('CHARLIE', 'CHARLES') # 0.71
std_lev('CHARLIE', 'CHAR') # 0.57
std_lev('CHARLIE', 'SANDRINE') # 0.25
std_lev('CHARLIE', None) # None

# How to use in a matchkey: HHID / YEAR / MONTH / STD LEV OF FIRST NAME > 0.70
matches = pd.merge(left=CEN,
                   right=PES,
                   how="inner",
                   left_on =['year_birth_cen', 'birth_month_cen', 'hhid_cen'],
                   right_on=['year_birth_pes', 'birth_month_pes', 'hhid_pes'])
matches['STD_EDIT'] = matches[['first_name_cen','first_name_pes']].apply(lambda x: std_lev(str(x[0]), str(x[1])), axis=1)
matches = matches[matches.STD_EDIT > 0.70]