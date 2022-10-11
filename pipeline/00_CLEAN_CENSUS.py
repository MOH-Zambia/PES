# Packages
import pandas as pd
import os
import sys

sys.path.insert(0, "../")
from lib.PARAMETERS import *

# Read in PILOT
CEN = pd.read_csv(CENSUS_FILE_PATH)

# Select columns needed throughout entire linkage process
CEN = CEN[['hid_cen', 'id_indi_cen', 'firstnm_cen', 'middlenm_cen', 'lastnm_cen', 'fullnm_cen', 'province_cen',
           'district_cen', 'sector_cen', 'cellule_cen', 'village_cen', 'month_cen', 'year_cen', 'age_cen',
           'HoH_cen', 'marstat_cen', 'marstatdesc_cen', 'relationship_cen', 'sex_cen', 'telephone_cen', 'lat_cen',
           'long_cen', 'full_loc_cen']]

# Loop through columns and change to string types
for column in ['province', 'district', 'sector', 'cellule', 'village', 'EA', 'building_number', 'hh_number',
               'person_id', 'marital_status', 'relationship_hh']:
    CEN[column] = CEN[column + '_cen'].astype(str)

# District / EA / HH / Person unique identifiers
CEN['DSid_cen'] = CEN['province_cen'] + CEN['district_cen']
CEN['EAid_cen'] = CEN['province_cen'] + CEN['district_cen'] + CEN['sector_cen'] + CEN['cellule_cen'] + \
                  CEN['village_cen'] + CEN['EA_cen']
CEN['hhid_cen'] = CEN['province_cen'] + CEN['district_cen'] + CEN['sector_cen'] + CEN['cellule_cen'] + \
                  CEN['village_cen'] + CEN['EA_cen'] + CEN['building_number_cen'] + CEN['hh_number_cen']
CEN['puid_cen'] = CEN['province_cen'] + CEN['district_cen'] + CEN['sector_cen'] + CEN['cellule_cen'] + \
                  CEN['village_cen'] + CEN['EA_cen'] + CEN['building_number_cen'] + CEN['hh_number_cen'] + \
                  CEN['person_id_cen']

# Save
CEN.to_csv(DATA_PATH + 'census_cleaned.csv', header=True, index=0)
