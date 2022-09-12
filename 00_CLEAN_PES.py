# Packages
import pandas as pd
import os
from lib.PARAMETERS import *

# Read in cleaned census
CEN = pd.read_csv(DATA_PATH + 'census_cleaned.csv')

# Sample of households for PES
pes_hh_sample = CEN['hhid_cen'].drop_duplicates().sample(200)

# Take sample from census
PES = pd.merge(CEN, pes_hh_sample, on='hhid_cen', how='inner')

# Remove '_cen' suffixes from PES columns
PES.columns = PES.columns.str.rstrip('_cen')

# PES Suffixes
PES = PES.add_suffix('_pes')

# Save PES before adding error
PES.to_csv(DATA_PATH + 'pes_cleaned.csv', header=True)

"""
After running this script, error should be added to the PES dataset manually, 
to make it harder to match the pilot to the synthetic PES.

Examples:
    - Swapped names
    - Missing values
    - Spelling mistakes
    - Mover households with different hhid / EAid values
"""
