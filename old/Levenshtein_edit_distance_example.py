# -*- coding: utf-8 -*-
"""
Created on Wed Feb  2 10:30:43 2022

@author: collys
"""
# A separate script as I couldn't figure out how to get this to work within a matchkey.
# Lev_merge function: Calculates a Levenshtein edit distance
# lev_name_example: the dataframe created by passing the Census and PES data through the lev_merge function
# display: a sample dataframe of name from Census and the PES names matched that are above the set threshold

import pandas as pd
import fuzzywuzzy as fz
import os

from fuzzywuzzy import process

import sys
sys.path.insert(0, "../")
import sys
sys.path.insert(0, "../")
from lib.PARAMETERS import *


# Read in synthetic data

df1 = pd.read_csv(CENSUS_FILE_PATH)
df2 = pd.read_csv(PES_FILE_PATH)
    
# You can also fuzzy match name using different string comparators. Here is an example of Levenshtein edit distance.
# For this example I have just demonstrated using names so you can see the potential candidates 
# for name returned. In deterministic linkage you would reduce the number of candidates by matching on
# other variables (e.g. birthdate, geography, etc.)
# N.B. I didn't write this function. It's from Stack Overflow, but demonstrates Lev edit distance nicely

def lev_merge(df_1, df_2, key1, key2, threshold=60, limit=10):
    """
    :param df_1: the left table to join
    :param df_2: the right table to join
    :param key1: key column of the left table
    :param key2: key column of the right table
    :param threshold: how close the matches should be to return a match, based on Levenshtein distance
    :param limit: the amount of matches that will get returned, these are sorted high to low
    :return: dataframe with boths keys and matches
    """
    s = df_2.to_dict()[key2]
    
    m = df_1[key1].apply(lambda x: process.extract(x, s, limit=limit))    
    df_1['matches'] = m
    
    m2 = df_1['matches'].apply(lambda x: ', '.join([i[0] for i in x if i[1] >= threshold]))
    df_1['matches'] = m2
    
    return df_1

lev_name_example = lev_merge(df1, df2, 'full_name', 'full_name', threshold = 60)
display = lev_name_example[['full_name', 'matches']].sample(20)
