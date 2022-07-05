import pandas as pd
import rapidfuzz
import math
import numpy as np

# ------------------------- #
# --------- DATA ---------- #
# ------------------------- #        

# Read in mock census and PES data
CEN = pd.read_csv('Data/Mock_Rwanda_Data_Census.csv')
PES = pd.read_csv('Data/Mock_Rwanda_Data_Pes.csv')

# select needed columns
CEN = CEN[['id_indi_cen', 'firstnm_cen', 'lastnm_cen', 'age_cen', 'month_cen', 'year_cen', 'sex_cen', 'province_cen']]
PES = PES[['id_indi_pes', 'firstnm_pes', 'lastnm_pes', 'age_pes', 'month_pes', 'year_pes', 'sex_pes', 'province_pes']]
    
# ----------------------------- #
# --------- BLOCKING ---------- #
# ----------------------------- #

# Block on province geographic variable
BP1 = 'province'

# Combine
for i, BP in enumerate([BP1], 1):
  
    if i == 1:
        combined_blocks = PES.merge(CEN, left_on = BP + '_pes', right_on = BP + '_cen', how = 'inner').drop_duplicates(['id_indi_cen', 'id_indi_pes'])
        print("1" + str(combined_blocks.count()))

# Count
len(combined_blocks) # 50042

# -------------------------------------------------- #
# --------------- AGREEMENT VECTORS ---------------- #
# -------------------------------------------------- #

# Agreement vector is created which is then inputted into the EM Algorithm.
# Set v1, v2,... vn as the agreement variables

# Select agreement variables
v1 = 'firstnm'
v2 = 'lastnm'
v3 = 'month'
v4 = 'year'
v5 = 'sex'

# All agreement variables used to calculate match weights & probabilities
all_variables = [v1, v2, v3, v4, v5]

# Variables using partial agreement (string similarity)
edit_distance_variables = [v1, v2]
dob_variables = [v3, v4]
remaining_variables = [v5]

# Cut off values for edit distance variables
cutoff_values = [0.45, 0.45]

# Replace NaN with blank spaces to assure the right data types for string similarity metrics
for variable in edit_distance_variables:
    cen_var = variable+ '_cen'
    pes_var = variable + '_pes'
    combined_blocks[cen_var] = combined_blocks[cen_var].fillna("")
    combined_blocks[pes_var] = combined_blocks[pes_var].fillna("")

def SLD(s,t):
    # Computing the standardised levenshtein edit distance between two strings 
    # using the rapidfuzz string matching library for it's fast string comparisons
    # Dividing result by 100 to return a score between 0 and 1
    standardised = (rapidfuzz.string_metric.normalized_levenshtein(s, t)/100)
    return standardised;

# Create forename/ last name Edit Distance score columns for all pairs
combined_blocks['firstnm_agreement'] = combined_blocks.apply(lambda x: SLD(x['firstnm_pes'], x['firstnm_cen']), axis=1)
combined_blocks['lastnm_agreement'] = combined_blocks.apply(lambda x: SLD(x['lastnm_pes'], x['lastnm_cen']), axis=1)
    
# --------------------------------------------------------- #
# ---------------- INITIAL M & U VALUES ------------------- #
# --------------------------------------------------------- #

# Read in M and U values
m_values = pd.read_csv('Data/m_values.csv')
u_values = pd.read_csv('Data/u_values.csv')

# Save individual M values from file
FN_M =  m_values[m_values.variable == 'firstnm'].iloc[0][1]
SN_M =  m_values[m_values.variable == 'lastnm'].iloc[0][1]
SEX_M = m_values[m_values.variable == 'sex'].iloc[0][1]
MONTH_M = m_values[m_values.variable == 'month'].iloc[0][1]
YEAR_M = m_values[m_values.variable == 'year'].iloc[0][1]

# Save individual U values from file
FN_U =  u_values[u_values.variable == 'firstnm'].iloc[0][1]
SN_U =  u_values[u_values.variable == 'lastnm'].iloc[0][1]
SEX_U = u_values[u_values.variable == 'sex'].iloc[0][1]
MONTH_U = u_values[u_values.variable == 'month'].iloc[0][1]
YEAR_U = u_values[u_values.variable == 'year'].iloc[0][1]

# Add M values to unlinked data
combined_blocks['firstnm_m'] = FN_M
combined_blocks['lastnm_m'] = SN_M
combined_blocks['sex_m'] = SEX_M
combined_blocks['month_m'] = MONTH_M
combined_blocks['year_m'] = YEAR_M

# Add U values to unlinked data
combined_blocks['firstnm_u'] = FN_U
combined_blocks['lastnm_u'] = SN_U
combined_blocks['sex_u'] = SEX_U
combined_blocks['month_u'] = MONTH_U
combined_blocks['year_u'] = YEAR_U

# Add Agreement / Disagreement Weights
for var in all_variables:
    
    # apply calculations: agreement weight = log base 2 (m/u)
    combined_blocks[var + "_agreement_weight"] = combined_blocks.apply(lambda x: (math.log2(x[var + "_m"] / x[var + "_u"])), axis = 1)
    
    # disagreement weight = log base 2 ((1-m)/(1-u))
    combined_blocks[var + "_disagreement_weight"] = combined_blocks.apply(lambda x: (math.log2((1 - x[var + "_m"]) / (1 - x[var + "_u"]))), axis = 1)
    
    # show sample of agreement/disagreement weights calculated
    print(combined_blocks[[var + "_m", var + "_u", var + "_agreement_weight", var + "_disagreement_weight"]].head(1))
    
'''
Alter the M and U values above (i.e. FN_M, FN_U etc. currently lines 100 - 112) to see the effect on variable agreement/disagreement weights
'''

# --------------------------------------------------- #
# ------------------ MATCH SCORES  ------------------ #
# --------------------------------------------------- #

''' An agreement value between 0 and 1 is calculated for each agreeement variable '''  
''' This is done for every candidate record pair '''  

# --------------------------------------- #
# ------------- DOB SCORE  -------------- #
# --------------------------------------- #  

# Partial scores
combined_blocks['month_agreement'] = np.where(combined_blocks['month_pes']  == combined_blocks['month_cen'],  1/3, 0)
combined_blocks['year_agreement'] = np.where(combined_blocks['year_pes']  == combined_blocks['year_cen'],  1/2, 0)

# Compute final Score and drop extra score columns
dob_score_columns = ['month_agreement', 'year_agreement']
combined_blocks['DOB_agreement'] = combined_blocks[dob_score_columns].sum(axis=1)
# combined_blocks = combined_blocks.drop(dob_score_columns, axis = 1)

# ---------------------------------------- #
# ---------- PARTIAL CUT OFFS ------------ #
# ---------------------------------------- #

# All partial variables except DOB
for variable, cutoff in zip(edit_distance_variables, cutoff_values):
  
    # If agreement below a certain level, set agreement to 0. Else, leave agreeement as it is
    combined_blocks[variable + '_agreement'] =  np.where(combined_blocks[variable + "_agreement"] <= cutoff, 0, combined_blocks[variable + "_agreement"])

# Remaining variables (no partial scores)
for variable in remaining_variables:
  
    # Calculate 1/0 Agreement Score (no partial scoring)
    combined_blocks[variable + '_agreement'] =  np.where(combined_blocks[variable + "_cen"] == combined_blocks[variable + "_pes"], 1, 0)
# ------------------------------------------------------------------ #
# -------------------------  WEIGHTS ------------------------------- #
# ------------------------------------------------------------------ #

# Start by giving all records agreement weights
for variable in all_variables:
    combined_blocks[variable + "_weight"] =  combined_blocks[variable + "_agreement_weight"]

# Update for partial agreement / disagreement (only when agreement < 1)
# source: https://www.census.gov/content/dam/Census/library/working-papers/1991/adrm/rr91-9.pdf
# weight = Agreement_Weight if Agreement = 1, and
#          MAX{(Agreement_Weight - (Agreement_Weight - Disgreement_Weight)*(1-Agreement)*(9/2)), Disgreement_Weight} if 0 <= Agreement < 1.

for variable in all_variables:
    combined_blocks[variable + "_weight"] = np.where(combined_blocks[variable + "_agreement"] < 1,
                                                      np.maximum(((combined_blocks[variable + "_agreement_weight"]) - 
                                                      ((combined_blocks[variable + "_agreement_weight"] - combined_blocks[variable + "_disagreement_weight"]) * 
                                                      (1 - combined_blocks[variable + "_agreement"]) * (9/2))),
                                                      combined_blocks[variable + "_disagreement_weight"]),
                                                      combined_blocks[variable + "_weight"])
    
# Set weights to 0 (instead of disagreement_weight) if there is missingess in PES or CEN variable (agreement == 0 condition needed for DOB)
for variable in all_variables:
    combined_blocks[variable + "_weight"] = np.where(combined_blocks[variable + '_pes'].isnull() | combined_blocks[variable + '_cen'].isnull() &
                                                                                               (combined_blocks[variable + '_agreement'] == 0), 0,
                                                                                               combined_blocks[variable + '_weight'])
    
# Sum column wise across the above columns - create match score
combined_blocks["match_score"] = combined_blocks[['firstnm_weight', 'lastnm_weight', 'month_weight', 'year_weight', 'sex_weight']].sum(axis=1)

# ------------------------------------------------------------------ #
# -----------------------  ADJUSTMENTS ----------------------------- #
# ------------------------------------------------------------------ #

# To reduce false matches going to clerical, if ages are dissimilar set score to 0
combined_blocks['match_score'] = np.where((combined_blocks['age_pes'].notnull() == False) & 
                                          combined_blocks['age_cen'].notnull() &
                                          (combined_blocks['age_pes'] - combined_blocks['age_cen'] > 5),
                                          0, combined_blocks['match_score'])

''' let's view some example clusters produced to check if the scores assigned are sensible'''

# high-scoring candidate record pairs
cen_vars = [s + '_cen' for s in all_variables]
pes_vars = [s + '_pes' for s in all_variables]

display(combined_blocks[cen_vars + pes_vars + ['match_score']].sort_values(by=['match_score'], ascending=False).head(50))

# and low-scoring candidate pairs
display(combined_blocks[cen_vars + pes_vars + ['match_score']].sort_values(by=['match_score']).head(50))

# -------------------------------------- #
# --------------  SAVE ----------------- #
# -------------------------------------- #

combined_blocks.to_csv('Data/Probabilistic_Scores.csv')
