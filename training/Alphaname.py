# Import any packages required
import pandas as pd
import numpy as np
import os
import sys

sys.path.insert(0, "../")
from lib.PARAMETERS import *

# Read in the census data
CEN = pd.read_csv(CENSUS_FILE_PATH, index_col=False)
print("Census read in")

# Read in the PES data
PES = pd.read_csv(PES_FILE_PATH, index_col=False)
print("PES read in")

# ------------------------------------------------------------------- #
# ----------------- CREATING 'ALPHANAME' VARIABLE  ------------------ #
# ------------------------------------------------------------------- #    

# Firsly, we need to replace missing names with empty spaces
CEN['first_name_temporary_cen'] = CEN['firstnm_cen'].fillna('')
CEN['last_name_temporary_cen'] = CEN['lastnm_cen'].fillna('')
PES['first_name_temporary_pes'] = PES['firstnm_pes'].fillna('')
PES['last_name_temporary_pes'] = PES['lastnm_pes'].fillna('')

# Now we can create fullname (no spaces) using the temporary name variables
CEN['fullname_ns_cen'] = CEN[['first_name_temporary_cen', 'last_name_temporary_cen']].apply(
                            lambda row: ''.join(row.values.astype(str)), axis=1)
PES['fullname_ns_pes'] = PES[['first_name_temporary_pes', 'last_name_temporary_pes']].apply(
                            lambda row: ''.join(row.values.astype(str)), axis=1)

# Create alphaname by sorting alphabetically
CEN['alpha_cen'] = [''.join(sorted(x)) for x in CEN['fullname_ns_cen']]
PES['alpha_pes'] = [''.join(sorted(x)) for x in PES['fullname_ns_pes']]

# Replace missing values with None
CEN['alpha_cen'] = np.where(CEN['alpha_cen'] == '', None, CEN['alpha_cen'])
PES['alpha_pes'] = np.where(PES['alpha_pes'] == '', None, PES['alpha_pes'])

# Drop temporary columns no longer needed
CEN = CEN.drop(['first_name_temporary_cen', 'last_name_temporary_cen', 'fullname_ns_cen'], axis=1)
PES = PES.drop(['first_name_temporary_pes', 'last_name_temporary_pes', 'fullname_ns_pes'], axis=1)

print(CEN[['firstnm_cen', 'lastnm_cen', 'alpha_cen']].head())

"""This code can be added to your cleaning scripts"""
