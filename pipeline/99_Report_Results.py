# Import any packages required
import pandas as 
import numpy as np
import sys

sys.path.insert(0, "C:\\Users\\tomlic\\Rwandan_linkage")
from lib.PARAMETERS import *

# Define Excel writer object and the target file
Excelwriter = pd.ExcelWriter("Results.xlsx",engine="xlsxwriter")

# Function for taking a dataframe, selecting required records and grouping by matchkey and clerical indicator
def results_func(file, match_type):
    """
    Records results from a specific matching stage and saves them in a new Excel spreadsheet
    :param file: input dataframe containing matches made from relevant matching method (within HH, within EA etc..)
    :param match_type: stage of matching that you want to filter the dataset on (matchkeys, associative, clerical matchkeys etc..)
    :return: results added to excel spreadsheet
    """
    # Read in matches
    df = pd.read_csv(OUTPUT_PATH + file + '.csv')
    
    # Select which stage of matching 
    df_stage = df[df['Match_Type'] == match_type]
    
    # Breakdown matches by matchkey and whether or not the match was made clerically
    results = pd.crosstab(df_stage['MK'], df_stage['CLERICAL'])
    
    # Convert to pandas DataFrame
    results.rename_axis(index = None, columns= results.index.name, inplace = True)
    results = results.reset_index(level=0)
    
    # Rename columns
    results.rename(columns={'index': 'Matchkey', 0: "Unique_Matches", 1: "Clerical_Matches"}, inplace = True)
    
    # Add to Excel spreadsheet
    results.to_excel(Excelwriter, sheet_name = match_type, index=False)
        
        
"""Apply function to different matching stages"""


# Within HH MKs    
results_func(file='Stage_1_All_Within_HH_Matches', match_type='Within_HH_Matchkey')
        
# Within HH Associative MKs    
results_func(file='Stage_1_All_Within_HH_Matches', match_type='Within_HH_Associative')

# Within EA MKs    
results_func(file='Stage_2_All_Within_EA_Matches', match_type='Within_EA_Matchkey')

# Within EA Associative MKs    
results_func(file='Stage_2_All_Within_EA_Matches', match_type='Within_EA_Associative')

# Within EA Clerical Matchkeys
results_func(file='Stage_3_Clerical_MK_EA_Matches', match_type='Within_EA_Clerical_MK')

# Within EA Search (in Excel)
results_func(file='Stage_4_Clerical_Search_EA_Matches', match_type='Within_EA_Clerical_Search')

# Within District MKs    
results_func(file='Stage_5_All_Within_DS_Matches', match_type='Within_DS_Matchkey')
        
# Within District Associative MKs    
results_func(file='Stage_5_All_Within_DS_Matches', match_type='Within_DS_Associative')

# Within Country MKs    
results_func(file='Stage_6_All_Within_Country_Matches', match_type='Within_Country_Matchkey')

# Within Country Associative MKs    
results_func(file='Stage_6_All_Within_Country_Matches', match_type='Within_Country_Associative')


"""Once matching has finished, we can take the final dataset and break it down by stage to get our final metrics
    (total matches, uniques, conflicts, match rate, unmatched PES etc.)"""


# Read in final matches
final_matches = pd.read_csv(OUTPUT_PATH + 'FINAL_MATCHES.csv')

# Stages in order
stages = ['Within_HH_Matchkey', 'Within_HH_Associative', 'Within_EA_Matchkey', 'Within_EA_Associative', 'Within_EA_Clerical_MK', 
         'Within_EA_Clerical_Search', 'Within_DS_Matchkey', 'Within_DS_Associative', 'Within_Country_Matchkey', 'Within_Country_Associative']

# Group by matching stage (Match_Type) and clerical indicator (similar to previous function)
df = pd.crosstab(final_matches['Match_Type'], final_matches['CLERICAL'])

# Convert to pandas DataFrame
df.rename_axis(index = None, columns= df.index.name, inplace = True)
df = df.reset_index(level=0)
df.rename(columns={'index': 'Stage', 0: "Unique_Matches", 1: "Clerical_Matches"}, inplace = True)
    
# Create a stage number column to sort data on
df['Stage_Number'] = None
for i, stage in enumerate(stages, 1):
    df['Stage_Number'] = np.where(df['Stage'] == stage, i, df['Stage_Number'])
df = df.sort_values(['Stage_Number'])

# Total PES and CEN (PES EAs) records
TOTAL_PES = 300
TOTAL_CEN_EA = 300

# Create columns for totals, cumulative totals, unmatched etc. 
df['Total_Matches'] = df['Unique_Matches'] + df['Clerical_Matches'] 
df['Cumulative_Unique_Matches'] = df['Unique_Matches'].cumsum()
df['Cumulative_PES_Match_Rate'] = df['Cumulative_Unique_Matches'] / TOTAL_PES * 100
df['Cumulative_Unmatched_PES'] = TOTAL_PES - df['Cumulative_Unique_Matches']
df['Cumulative_Unmatched_CEN'] = TOTAL_CEN_EA - df['Cumulative_Unique_Matches']

# Column order
df = df[['Stage_Number', 'Stage', 'Total_Matches', 'Unique_Matches', 'Clerical_Matches',
       'Cumulative_Unique_Matches','Cumulative_PES_Match_Rate', 'Cumulative_Unmatched_PES','Cumulative_Unmatched_CEN']]

# Save DataFrame to Excel spreadsheet
df.to_excel(Excelwriter, sheet_name = 'Final_Metrics', index=False)
      
# Save spreadsheet
Excelwriter.save()
