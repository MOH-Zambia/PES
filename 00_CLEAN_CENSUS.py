# Packages
import pandas as pd
import os
from lib.PARAMETERS import *

# Read in PILOT
CEN = pd.read_stata(DATA_PATH + '/PHC5_Pilot/Pilot_Structure/phc5.dta', convert_categoricals=False)

# Select columns needed throughout entire linkage process
CEN = CEN[
    ['ml01', 'ml02', 'ml03', 'ml04', 'ml05', 'ml06', 'ml08', 'ml09', 'pid', 'p01_name', 'p02', 'p03', 'p04', 'p05a',
     'p05b', 'p06']]

# Renaming columns
CEN.rename(columns={'ml01': 'province', 'ml02': 'district', 'ml03': 'sector', 'ml04': 'cell',
                    'ml05': 'village', 'ml06': 'EA', 'ml08': 'building_number', 'ml09': 'hh_number',
                    'pid': 'person_id', 'p01_name': 'names', 'p02': 'relationship_hh', 'p03': 'sex',
                    'p04': 'age', 'p05a': 'birth_month', 'p05b': 'year_birth', 'p06': 'marital_status'}, inplace=True)

# Loop through columns and change to string types
for column in ['province', 'district', 'sector', 'cell', 'village', 'EA', 'building_number', 'hh_number', 'person_id',
               'marital_status', 'relationship_hh']:
    CEN[column] = CEN[column].astype(str)

# District / EA / HH / Person unique identifiers
CEN['DSid'] = CEN['province'] + CEN['district']
CEN['EAid'] = CEN['province'] + CEN['district'] + CEN['sector'] + CEN['cell'] + CEN['village'] + CEN['EA']
CEN['hhid'] = CEN['province'] + CEN['district'] + CEN['sector'] + CEN['cell'] + CEN['village'] + CEN['EA'] + CEN[
    'building_number'] + CEN['hh_number']
CEN['puid'] = CEN['province'] + CEN['district'] + CEN['sector'] + CEN['cell'] + CEN['village'] + CEN['EA'] + CEN[
    'building_number'] + CEN['hh_number'] + CEN['person_id']

# Derive first, second & last names
CEN['first_name'] = CEN['names'].str.split().str.get(0)
CEN['second_name'] = CEN['names'].str.split().str.get(1)
CEN['last_name'] = CEN['names'].str.split().str.get(-1)

# Save census data
CEN = CEN.add_suffix('_cen')

# Save
CEN.to_csv(DATA_PATH + 'census_cleaned.csv', header=True)
