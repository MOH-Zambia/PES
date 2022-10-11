# Packages
import pandas as pd
import os
import sys

sys.path.insert(0, "../")
from lib.PARAMETERS import *

# Read in PILOT
PES = pd.read_csv(PES_FILE_PATH)

# Select columns needed throughout entire linkage process
PES = PES[['hid_pes', 'id_indi_pes', 'firstnm_pes', 'middlenm_pes', 'lastnm_pes', 'fullnm_pes', 'province_pes',
           'district_pes', 'sector_pes', 'cellule_pes', 'village_pes', 'month_pes', 'year_pes', 'age_pes', 'HoH_pes',
           'marstat_pes', 'marstatdesc_pes', 'relationship_pes', 'sex_pes', 'telephone_pes', 'lat_pes', 'long_pes',
           'full_loc_pes']]

# Save PES before adding error
PES.to_csv(DATA_PATH + 'pes_cleaned.csv', header=True, index=0)

"""
After running this script, error should be added to the PES dataset manually, 
to make it harder to match the pilot to the synthetic PES.

Examples:
    - Swapped names
    - Missing values
    - Spelling mistakes
    - Mover households with different hhid / EAid values
"""
