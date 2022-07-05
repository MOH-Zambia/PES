# -*- coding: utf-8 -*-
"""
Created on Thu Jan 20 14:25:54 2022

@author: collys
"""

" Rwanda synthetic data - deterministic linkage example"

""" Matching vars - 
    Full_name
    First_name
    Last_name
    Month
    Year
    Sex
    Age
    Head_of_household
    Telephone_number
    Province
    District
    Sector
    Cell
    Village 
    Latitude
    Longitude"""
    
# A couple of notes - not all of this script runs as I couldn't figure out how to convert some of the Pyspark
# code to Python in time

# Function that creates matchkeys - This will run and demonstrates the different conditions we can link on.
# Matching loop - doesn't run; have annotated to explain the logic.
# Conflict resolution - doesn't run; have annotated to explain the logic.

import pandas as pd
import numpy as np
import fuzzywuzzy as fz
from fuzzywuzzy import process
import jellyfish


# Read in synthetic data

df1 = pd.read_csv('Data/Mock_Rwanda_Data_Census.csv')
df2 = pd.read_csv('Data/Mock_Rwanda_Data_Pes.csv')

# Need to add some empty rows to the Census data (df1) to get the matchkeys to run
df1.loc[df1.shape[0]] = [None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None] 
df1.loc[df1.shape[0]] = [None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None] 
df1.loc[df1.shape[0]] = [None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None] 

# list of matchkeys. These are the conditions to loop through and make our matches.
# This function does run and produces a list of keys that would be passed to a matching loop

def MATCHKEYS(df1, df2):

# Exact match on Full Name, Head of Household, Month & Year of Birth, Village    
    mk1 = [df1['full_name'] == df2['full_name'],
           df1['HoH'] == df2['HoH'],
           df1['month'] == df2['month'],
           df1['year'] == df2['year'],
           df1['lati'] == df2['lati'],
           df1['lon'] == df2['lon']]

# Exact match on Full Name, Head of Household, Age, Village 
    mk2 = [df1.full_name == df2.full_name,
           df1.HoH == df2.HoH,
           df1['age.y'] == df2['age.y'],
           df1.lati == df2.lati,
           df1.lon == df2.lon]

# Exact match on First Name, Last Name, Head of Household, Month & Year of Birth, Village
    mk3 = [df1.first_name == df2.first_name,
           df1.last_name == df2.last_name,
           df1.HoH == df2.HoH,
           df1.month == df2.month,
           df1.year == df2.year,
           df1.lati == df2.lati,
           df1.lon == df2.lon]

# Exact match on First Name, Last Name, Head of Household, Age, Village
    mk4 = [df1.first_name == df2.first_name,
           df1.last_name == df2.last_name,
           df1.HoH == df2.HoH,
           df1['age.y'] == df2['age.y'],
           df1.lati == df2.lati,
           df1.lon == df2.lon]
    
 # Fuzzy name - first 3 characters from first and last name
   
    mk5 = [df1.first_name[0:3] == df2.first_name[0:3],
       df1.last_name[0:3] == df2.last_name[0:3],
       df1.month == df2.month,
           df1.year == df2.year,
           df1.lati == df2.lati,
           df1.lon == df2.lon]

# Fuzzy age/DoB (for the actual linkage we want to scale the difference in age depending on the age
# but for this example I've just set it to be a difference of 3 or fewer)

    mk6 = [df1.full_name == df2.full_name,
           df1.HoH == df2.HoH,
           (df1['age.y'] - df2['age.y']) < 3,
           df1.lati == df2.lati,
           df1.lon == df2.lon]

    mk7 = [df1.full_name == df2.full_name,
           (df1.year - df2.year) < 3,
           df1.lati == df2.lati,
           df1.lon == df2.lon] 

    mk8 = [df1.full_name == df2.full_name,
           df1.HoH == df2.HoH,
           (df1.year - df2.year) < 3,
           df1.village == df2.village] 

# Fuzzy geography - use different levels of geography (village, cell, sector). The looser we go on
# geography the tighter we want the rest of the matching conditions to be to prevent false positives

    mk9 = [df1.full_name == df2.full_name,
           df1.HoH == df2.HoH,
           df1['age.y'] == df2['age.y'],
           df1.village == df2.village]

    mk10 = [df1.full_name == df2.full_name,
            df1.HoH == df2.HoH,
            df1['age.y'] == df2['age.y'],
            df1.cell == df2.cell]

    mk11 = [df1.full_name == df2.full_name,
            df1.HoH == df2.HoH,
            df1.month == df2.month,
            df1.year == df2.year,
            df1.sector == df2.sector]

    
    keys = [mk1, mk2, mk3, mk4, mk5, mk6, mk7, mk8, mk9, mk10, mk11]
    
    return keys

# Matching - I haven't figured out how to un-Pyspark it properly, but I have annotated to demonstrate
# what it should be doing.

# Create a list of columns you want to keep in the matched file
columns = ['id_indi_x', 'id_indi_y', 'mkey']

# Create an empty list/dataframe that you will append your links to
matches = []

# Matching loop 

for i, EX in (keys, 1):
    
    print("\n MATCHKEY", i) # prints out which matchkey the loop is currently running
    
    # Join on blocking pass i (joins on each condition)
    df = df1.merge(df2, on = EX, how = 'inner')
    
    # Create the KEY column (creates the mkey column)
    df = df.insert('mkey')
    
    # Append pairs to final dataset (unions together the data from each pass)
    matches = matches.append(df)
    
    # Minimum blocking pass for every unique PAIR (creates a new column showing the minimum mkey for each
                                                   # unique pair of ID's (Census and PES ID))
    matches = matches.insert('Min_Key_Match', min('mkey').over(Window.partitionBy('id_indi_x', 'id_indi_y')))
    
    # Drops any pairs (records) where the value in the mkey column does not match the value in the minimum
    # matchkey column. Essentially this keeps only the best matches.
    matches = matches.filter(matches.mkey == matches.Min_key_match).drop('Min_Key_match') 
    
    print(matches.count()) # prints out count of data for each matchkey. this is cumulative as the loop runs
    
# This section also doesn't run, because the above loop doesn't run,
# and this is PySpark code, not Python. Again I have annotated what the purpose is!

# Between key conflicts (non-unique links)

# Calculates the minimum key for each Census ID
matches = matches.withColumn('min_key_cen', F.min('mkey').over(Window.partitionBy('id_cen')))
# Calculates the minimum key for each PES ID
matches = matches.withColumn('min_key_pes', F.min('mkey').over(Window.partitionBy('id_pes'))) 
# Filters out any pairs of records where either Census or PES record has been matched by an earlier matchkey   
matches = matches.filter(((matches['min_key_cen'] == matches['mkey']) & 
                         (matches['min_key_pes'] == matches['mkey']) | (matches.mkey <5)))

# Within key non-uniques

# Calculates how many Census records links there are by each PES ID
matches = matches.withColumn('match_count_cen', F.approx_count_distinct('id_pes').over(Window.partitionBy('id_cen')))
# Calculates how many PES records links there are by each Census ID
matches = matches.withColumn('match_count_pes', F.approx_count_distinct('id_cen').over(Window.partitionBy('id_pes')))
# Filters out duplicate records (i.e. records where the ID count does not equal 1)
matches = matches.filter((matches['match_count_cen'] == 1) & 
                         (matches['match_count_pes'] == 1))   
    
    
