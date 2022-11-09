# Import any packages required
import pandas as pd
import numpy as np
import jellyfish
import os
import sys
import openpyxl
import xlsxwriter

sys.path.insert(0, "C:\\Users\\tomlic\\Rwandan_linkage")
from lib.PARAMETERS import *

# Define Excel writer object and the target file
Excelwriter = pd.ExcelWriter("Results.xlsx",engine="xlsxwriter")

# ----- HH ----- #
df_hh = pd.read_csv(OUTPUT_PATH + 'Stage_1_All_Within_HH_Matches.csv')
df1 = df_hh[df_hh['Match_Type'] == 'Within_HH_Matchkey']
results1 = pd.crosstab(df1['MK'], df1['CLERICAL'])
results1.rename_axis(index = None, columns= results1.index.name, inplace = True)
results1 = results1.reset_index(level=0)
results1.rename(columns={'index': 'Matchkey', 0: "Unique_Matches", 1: "Clerical_Matches"}, inplace = True)

# HH ASSOCIATIVE
df2 = df_hh[df_hh['Match_Type'] == 'Within_HH_Associative']
results2 = df2['MK'].value_counts().rename_axis('MK').reset_index(name='Unique_Matches')

# EA
df_ea = pd.read_csv(OUTPUT_PATH + 'Stage_2_All_Within_EA_Matches.csv')
df3 = df_ea[df_ea['Match_Type'] == 'Within_EA_Matchkey']
results3 = pd.crosstab(df3['MK'], df3['CLERICAL'])
results3.rename_axis(index = None, columns= results3.index.name, inplace = True)
results3 = results3.reset_index(level=0)
results3.rename(columns={'index': 'Matchkey', 0: "Unique_Matches", 1: "Clerical_Matches"}, inplace = True)

# EA ASSOCIATIVE
df4 = df_ea[df_ea['Match_Type'] == 'Within_EA_Associative']
results4 = df4['MK'].value_counts().rename_axis('MK').reset_index(name='Unique_Matches')

# EA CLERICAL MATCHKEYS

# EA CLERICAL SEARCH (EXCEL)

# DISTRICT

# COUNTRY

# Save results
results_save = [results1, results2]
sheet_names = ['Within_HH', 'Associative_HH']
for result, sheet in zip(results_save, sheet_names):
    result.to_excel(Excelwriter, sheet_name= sheet, index=False)
    
# Save
Excelwriter.save()

