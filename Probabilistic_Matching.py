import pandas as pd
import rapidfuzz
import math
import numpy as np

# ------------------------- #
# --------- DATA ---------- #
# ------------------------- #        

# Read in mock census and PES data
CEN = pd.read_csv('Mock_Rwanda_Data_Census.csv')
PES = pd.read_csv('Mock_Rwanda_Data_Pes.csv')

# ----------------------------- #
# --------- BLOCKING ---------- #
# ----------------------------- #

# Block on geographic variables
BP1 = 'village'
BP2 = 'cellule'
BP3 = 'sector'
BP4 = 'district'
BP5 = 'province'

# Combine
for i, BP in enumerate([BP1,BP2,BP3,BP4,BP5], 1):
  
  if i == 1:
    combined_blocks = PES.merge(CEN, left_on = BP + '_pes', right_on = BP + '_cen', how = 'inner')
    print(combined_blocks.count())
    
  if i > 1:
    combined_blocks_2 = PES.merge(CEN, left_on = BP + '_pes', right_on = BP + '_cen', how = 'inner')
    print(combined_blocks_2.count())
    combined_blocks = pd.concat([combined_blocks, combined_blocks_2]).drop_duplicates(['id_indi_cen', 'id_indi_pes'])

# Count
len(combined_blocks) # 44968

# -------------------------------------------------- #
# --------------- AGREEMENT VECTORS ---------------- #
# -------------------------------------------------- #

# Agreement vector is created which is then inputted into the EM Algorithm.
# Set v1, v2,... vn as the agreement variables

# Select agreement variables
v1 = 'firstnm'
v2 = 'lastnm'
v3 = 'sex'

# All agreement variables used to calculate match weights & probabilities
all_variables = [v1, v2, v3]

# Variables using partial agreement (string similarity)
edit_distance_variables = [v1, v2]

remaining_variables = [v3]

# Cut off values for edit distance variables
cutoff_values = [0.45, 0.45]
dob_cutoff_values = [0.50]

# Remaining Variables - Only zero or full agreement for these
# remaining_variables = [v4]

# Replace NaN with blank spaces to assure the right data types for string similarity metrics
for variable in edit_distance_variables:
    cen_var = variable+ '_cen'
    pes_var = variable + '_pes'
    combined_blocks[cen_var] = combined_blocks[cen_var].fillna("")
    combined_blocks[pes_var] = combined_blocks[pes_var].fillna("")

def SLD(s,t):
    # Using the rapidfuzz string matching library for it's fast string comparisons
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
m_values = pd.read_csv('m_values.csv')
u_values = pd.read_csv('u_values.csv')

# Save individual M values
FN_M =  m_values[m_values.variable == 'firstnm'].iloc[0][1]
SN_M =  m_values[m_values.variable == 'lastnm'].iloc[0][1]
SEX_M = m_values[m_values.variable == 'sex'].iloc[0][1]

# Save individual U values
FN_U =  u_values[u_values.variable == 'firstnm'].iloc[0][1]
SN_U =  u_values[u_values.variable == 'lastnm'].iloc[0][1]
SEX_U = u_values[u_values.variable == 'sex'].iloc[0][1]

# Add M values back into file
combined_blocks['firstnm_m'] = FN_M
combined_blocks['lastnm_m'] = SN_M
combined_blocks['sex_m'] = SEX_M

# Add U values back into file
combined_blocks['firstnm_u'] = FN_U
combined_blocks['lastnm_u'] = SN_U
combined_blocks['sex_u'] = SEX_U

# Add Agreement / Disagreement Weights
for var in all_variables:
    print(var)
    combined_blocks[var + "_agreement_weight"] = combined_blocks.apply(lambda x: (math.log2(x[var + "_m"] / x[var + "_u"])), axis = 1)
    combined_blocks[var + "_disagreement_weight"] = combined_blocks.apply(lambda x: (math.log2((1 - x[var + "_m"]) / (1 - x[var + "_u"]))), axis = 1)
                             
# --------------------------------------------------- #
# ------------------ MATCH SCORES  ------------------ #
# --------------------------------------------------- #

''' An agreement value between 0 and 1 is calculated for each agreeement variable '''  
''' This is done for every candidate record pair '''  

# --------------------------------------- #
# ------------- DOB SCORE  -------------- #
# --------------------------------------- #  

# Partial scores
# combined_blocks['day_score'] = ,   when(combined_blocks.day_ccs  == combined_blocks.day_cen,  1/3).otherwise(0))
# combined_blocks['day_score'] = combined_blocks.withColumn('month_score', when(combined_blocks.mon_ccs  == combined_blocks.mon_cen,  1/3).otherwise(0))
# combined_blocks['day_score'] = combined_blocks.withColumn('year_score',  when(combined_blocks.year_ccs == combined_blocks.year_cen, 1/3).otherwise(0))

# # Final Score
# combined_blocks = combined_blocks.withColumn('DOB_agreement', F.expr("day_score + month_score + year_score")).drop('day_score', 'month_score', 'year_score')

# # DOB Agreement Adjustment for strong candidates with missing DOB and AGE within 3
# combined_blocks = combined_blocks.withColumn('DOB_agreement', 
#                                              when(((abs_(combined_blocks.age_ccs - combined_blocks.age_cen) < 4) & 
#                                                    (combined_blocks.fn1_agreement > 0.65) & (combined_blocks.sn1_agreement > 0.65) &
#                                                    (combined_blocks.sex_cen == combined_blocks.sex_ccs) &
#                                                    (combined_blocks.dob_cen.isNull() | combined_blocks.dob_ccs.isNull())), lit(0.55)).otherwise(col('DOB_agreement')))

# # DOB Agreement Adjustment for strong candidates with missing DOB and AGE within 1
# combined_blocks = combined_blocks.withColumn('DOB_agreement', 
#                                              when(((abs_(combined_blocks.age_ccs - combined_blocks.age_cen) < 2) & 
#                                                    (combined_blocks.fn1_agreement > 0.65) & (combined_blocks.sn1_agreement > 0.65) &
#                                                    (combined_blocks.sex_cen == combined_blocks.sex_ccs) &
#                                                    (combined_blocks.dob_cen.isNull() | combined_blocks.dob_ccs.isNull())), lit(0.7)).otherwise(col('DOB_agreement')))

# ---------------------------------------- #
# ---------- PARTIAL CUT OFFS ------------ #
# ---------------------------------------- #

# All partial variables except DOB
for variable, cutoff in zip(edit_distance_variables, cutoff_values):
  
    # If agreement below a certain level, set agreement to 0. Else, leave agreeement as it is
    combined_blocks[variable + '_agreement'] =  np.where(combined_blocks[variable + "_agreement"] <= cutoff, 0, combined_blocks[variable + "_agreement"])

# # DOB
# for variable, cutoff in zip(dob_variables, dob_cutoff_values):
  
#   # If agreement below a certain level, set agreement to 0. Else, leave agreeement as it is
#   combined_blocks = combined_blocks.withColumn(variable + "_agreement", when(col(variable + "_agreement") <= cutoff, 0).otherwise(col(variable + "_agreement")))

# Remaining variables (no partial scores)
for variable in remaining_variables:
  
    # Calculate 1/0 Agreement Score (no partial scoring)
    combined_blocks[variable + '_agreement'] =  np.where(combined_blocks[variable + "_cen"] == combined_blocks[variable + "_pes"], 1, 0)
# ------------------------------------------------------------------ #
# -------------------------  WEIGHTS ------------------------------- #
# ------------------------------------------------------------------ #

# https://www.census.gov/content/dam/Census/library/working-papers/1991/adrm/rr91-9.pdf
# weight = Agreement_Weight if Agreement = 1, and
#          MAX{(Agreement_Weight - (Agreement_Weight - Disgreement_Weight)*(1-Agreement)*(9/2)), Disgreement_Weight} if 0 <= Agreement < 1.

# Start by giving all records agreement weights
for variable in all_variables:
    combined_blocks[variable + "_weight"] =  combined_blocks[variable + "_agreement_weight"]

# Update for partial agreement / disagreement (only when agreement < 1)
for variable in all_variables:
    combined_blocks[variable + "_weight"] = np.where(combined_blocks[variable + "_agreement"] < 1,
                                                   max((combined_blocks[variable + "_agreement_weight"] - (combined_blocks[variable + "_agreement_weight"] - 
                                                   combined_blocks[variable + "_disagreement_weight"]) * (1 - combined_blocks[variable + "_agreement_weight"]) * (2)),
                                                   combined_blocks[variable + "_disagreement_weight"]),
                                                   combined_blocks[variable + "_weight"])

# Set weights to 0 (instead of disagreement_weight) if there is missingess in CCS or CEN variable (agreement == 0 condition needed for DOB)
for variable in all_variables:
    combined_blocks[variable + "_weight"] = np.where(combined_blocks[variable + '_pes'].isna | combined_blocks[variable + '_cen'].isna &
                                                                                               (combined_blocks[variable + '_agreement'] == 0), 0,
                                                                                               combined_blocks[variable + '_weight'])
    
# Sum column wise across the above columns - create match score
combined_blocks["match_score"] = sum(combined_blocks["fn1_weight", "sn1_weight", "dob_weight", "sex_weight"])

# ------------------------------------------------------------------ #
# -----------------------  ADJUSTMENTS ----------------------------- #
# ------------------------------------------------------------------ #

# To reduce false matches going to clerical, if ages are dissimilar set score to 0
combined_blocks = combined_blocks.withColumn("match_score", when(((col('age_ccs').isNotNull()) & 
                                                                  (col('age_cen').isNotNull()) &
                                                                  (abs_(combined_blocks.age_ccs - combined_blocks.age_cen) > 5)), lit(0)).otherwise(col('match_score')))
# -------------------------------------- #
# --------------  SAVE ----------------- #
# -------------------------------------- #

combined_blocks.to_csv('Probabilistic_Scores')