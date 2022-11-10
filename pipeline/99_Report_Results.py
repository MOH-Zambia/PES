# Import any packages required
import pandas as pd
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

# Save spreadsheet
Excelwriter.save()