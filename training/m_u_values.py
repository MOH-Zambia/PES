import pandas as pd
import numpy as np
import sys

sys.path.insert(0, "../")
from lib.PARAMETERS import *

# ------------------------------------- #
# -- Create PES & CEN gold standard --- #
# ------------------------------------- #

# Read in mock census and PES data
CEN = pd.read_csv(CENSUS_FILE_PATH)
PES = pd.read_csv(PES_FILE_PATH)

# join on unique ID
gold_standard = CEN.merge(PES, left_on='id_indi_cen', right_on='id_indi_pes', how='inner')

# --------------------------- #
# --------- M VALUES -------- #
# --------------------------- #

# Create empty dataframe to add m values to
m_values = pd.DataFrame([])

# Probabilistic linkage variables
MU_variables = ['firstnm', 'lastnm', 'sex', 'month', 'year']

# Store total number of records for use in calculation
total_records = len(gold_standard)

# --- for loop --- #

# For each variable:
for v in MU_variables:
    print(v)

    # Remove missing rows
    gold_standard.dropna(subset=[v + '_cen'], inplace=True)
    gold_standard.dropna(subset=[v + '_pes'], inplace=True)

    # counting total number of non-missing probabilistic variables
    total_records = len(gold_standard)

    # Create a column that stores whether there is exact agreement for that pair
    gold_standard[v + "_exact"] = np.where(gold_standard[v + '_pes'] == gold_standard[v + '_cen'], 1, 0)

    # Use the sum_col function to create a total number of pairs with exact agreement
    exact = gold_standard[v + "_exact"].sum()

    # Divide the total number of exact matches by the total number of records
    value = exact / total_records

    # Store the results in a data frame
    m_values = m_values.append(pd.DataFrame({'variable': v, 'm_value': value}, index=[1]), ignore_index=True)

print(m_values)

# ------------------------------ #
# ---------- U VALUES ---------- #
# ------------------------------ #

# ----- Sample for calculating U values from full census ----- #

# DataFrame to append to
u_values = pd.DataFrame([])

# condition to reset loop if u value is 0
restart = True
while restart:

    # For name variables:
    for v in MU_variables:

        # Randomly sort datasets
        CEN = CEN.sample(frac=1).reset_index(drop=True)
        PES = PES.sample(frac=1).reset_index(drop=True)

        # Add a ID column to join on
        sample = pd.merge(CEN, PES, left_index=True, right_index=True)

        # Remove missing rows
        sample.dropna(subset=[v + '_cen'], inplace=True)
        sample.dropna(subset=[v + '_pes'], inplace=True)

        # Count
        total = len(sample)

        # Agreement count
        sample[v + "_exact"] = np.where(sample[v + '_pes'] == sample[v + '_cen'], 1, 0)

        # Create a total number of pairs with exact agreement
        exact = sample[v + "_exact"].sum()

        # Proportion
        value = exact / total

        # condition to reset loop if u value is 0
        if value > 0 and v != 'year':

            # Append to DataFrame
            u_values = u_values.append(pd.DataFrame({'variable': v, 'u_value': value}, index=[1]), ignore_index=True)

            # Add DOB U value if needed
            # u_values = u_values.append(pd.DataFrame(data = ({'u_value': [(1/(365*80)) * 100], 'variable': ['dob']})), ignore_index = True)

        else:
            restart = False
            break

# Print
print(u_values)

# ------------------------------------- #
# --------------- SAVE ---------------- #
# ------------------------------------- #

# Spark DataFrame
m_values.to_csv(DATA_PATH + '/m_values.csv', header=True, index=False)
u_values.to_csv(DATA_PATH + '/u_values.csv', header=True, index=False)
