# ------------------------------------------------------------------- #
# ------------- STANDARDISED LEVENSHTEIN EDIT DISTANCE -------------- #
# ------------------------------------------------------------------- # 

# Import any packages required
import jellyfish
import pandas as pd
import sys

sys.path.insert(0, "../")
from lib.PARAMETERS import *


def std_lev(string1, string2):
    """
    Function to calculate the standardised Levenshtein edit distance between two strings.
    :param string1: field1
    :param string2: field2
    :return: score between 0 and 1
    """
    # Score = None if strings are None
    if string1 is None or string2 is None:
        return None

    # Calculate score if strings are not None
    else:
        # String lengths
        length1, length2 = len(string1), len(string2)

        # Max length
        max_length = max(length1, length2)

        # Edit Distance
        lev = jellyfish.levenshtein_distance(string1, string2)

        # Standardised Edit Distance
        std_lev = 1 - (lev / max_length)

        return std_lev


# Test
std_lev('CHARLIE', 'CHARLIE')  # 1.00
std_lev('CHARLIE', 'CHARLES')  # 0.71
std_lev('CHARLIE', 'CHAR')  # 0.57
std_lev('CHARLIE', 'SANDRINE')  # 0.25
std_lev('CHARLIE', None)  # None

# How to use in a matchkey: HHID / YEAR / MONTH / STD LEV OF FIRST NAME > 0.70

CEN = pd.read_csv(CENSUS_FILE_PATH)
PES = pd.read_csv(PES_FILE_PATH)
matches = pd.merge(left=CEN,
                   right=PES,
                   how="inner",
                   left_on=['year_cen', 'month_cen', 'id_indi_cen'],
                   right_on=['year_pes', 'month_pes', 'id_indi_pes'])
matches['STD_EDIT'] = matches[['firstnm_cen', 'firstnm_pes']].apply(lambda x: std_lev(str(x[0]), str(x[1])),
                                                                    axis=1)
matches = matches[matches.STD_EDIT > 0.70]
print(matches.head())
