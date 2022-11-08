# Packages
import pandas as pd
import os
import sys

os.chdir("C:/Users/tomlic/Rwandan_linkage/")

sys.path.insert(0, "../")
from lib.PARAMETERS import *

# Read in PILOT
PES = pd.read_csv(PES_FILE_PATH)

# Select columns needed throughout entire linkage process
PES = PES[['hid_pes', 'puid_pes', 'forename_pes', 'middlenm_pes', 'last_name_pes', 'names_pes', 'province_pes',
           'district_pes', 'sector_pes', 'cellule_pes', 'village_pes', 'month_pes', 'year_pes', 'age_pes', 'HoH_pes',
           'marstat_pes', 'marstatdesc_pes', 'relationship_pes', 'sex_pes', 'telephone_pes', 'lat_pes', 'long_pes',
           'full_loc_pes', "EAid_pes", "DSid_pes"]]

# Convert variables to strings
for column in ['hid_pes', 'puid_pes', 'forename_pes', 'middlenm_pes', 'last_name_pes', 'names_pes', 'province_pes',
               'district_pes', 'sector_pes', 'cellule_pes', 'village_pes', 'month_pes', 'year_pes', 'age_pes', 'HoH_pes',
               'marstat_pes', 'marstatdesc_pes', 'relationship_pes', 'sex_pes', 'telephone_pes', 'lat_pes', 'long_pes',
               'full_loc_pes', "EAid_pes", "DSid_pes"]:
    PES[column] = PES[column].astype(str)

# Dervive DOB
PES["dob_pes"] = PES["month_pes"] + "/" + PES["year_pes"]

# Remove duplicate puids
PES = PES.drop_duplicates(['puid_pes'])

# Add 'PES_' infront of all puids
PES['puid_pes'] = 'PES_' + PES['puid_pes']

# Save PES before adding error
PES.to_csv(DATA_PATH + 'pes_cleaned.csv', header=True, index=0)
print("Cleaned pes and save as %s" % DATA_PATH + 'pes_cleaned.csv')

"""
After running this script, error should be added to the PES dataset manually, 
to make it harder to match the pilot to the synthetic PES.

Examples:
    - Swapped names
    - Missing values
    - Spelling mistakes
    - Mover households with different hhid / EAid values
"""
