import pandas as pd
import jellyfish


def matchkey_1(CEN, PES, id_cen='EAid_cen', id_pes='EAid_pes'):
    """
    Matchkey 1: Full Name + Year + Month + EA/HH
    """
    matches_1 = pd.merge(left=CEN,
                         right=PES,
                         how="inner",
                         left_on=['fullnm_cen', 'year_cen', 'month_cen', id_cen],
                         right_on=['fullnm_pes', 'year_pes', 'month_pes', id_pes])
    return matches_1


def matchkey_2(CEN, PES, edit_distance_limit=2, id_cen='EAid_cen', id_pes='EAid_pes'):
    """
    Matchkey 2: Edit Distance < 2 + Year + Month + EA/HH
    """
    matches_2 = pd.merge(left=CEN,
                         right=PES,
                         how="inner",
                         left_on=['year_cen', 'month_cen', id_cen],
                         right_on=['year_pes', 'month_pes', id_pes])
    matches_2['EDIT'] = matches_2[['fullnm_cen', 'fullnm_pes']].apply(
        lambda x: jellyfish.levenshtein_distance(str(x[0]), str(x[1])), axis=1)
    matches_2 = matches_2[matches_2.EDIT < edit_distance_limit]
    return matches_2


def matchkey_3(CEN, PES, id_cen='EAid_cen', id_pes='EAid_pes'):
    """
    Matchkey 3: Full Name + Year + Sex + EA/HH
    """
    matches_3 = pd.merge(left=CEN,
                         right=PES,
                         how="inner",
                         left_on=['fullnm_cen', 'year_cen', 'sex_cen', id_cen],
                         right_on=['fullnm_pes', 'year_pes', 'sex_pes', id_pes])
    return matches_3


def matchkey_4(CEN, PES, id_cen='EAid_cen', id_pes='EAid_pes'):
    """
    Matchkey 4: Full Name + Age + Sex + EA/HH
    """

    matches_4 = pd.merge(left=CEN,
                         right=PES,
                         how="inner",
                         left_on=['fullnm_cen', 'age_cen', 'sex_cen', id_cen],
                         right_on=['fullnm_pes', 'age_pes', 'sex_pes', id_pes])
    return matches_4


def matchkey_5(CEN, PES, relationship_cen='relationship_hh_cen', relationship_pes='relationship_hh_pes',
               id_cen='EAid_cen', id_pes='EAid_pes'):
    """
    Matchkey 5: Allowing age/year/month to be different
    """
    matches_5 = pd.merge(left=CEN,
                         right=PES,
                         how="inner",
                         left_on=['fullnm_cen', 'sex_cen', relationship_cen, id_cen],
                         right_on=['fullnm_pes', 'sex_pes', relationship_pes, id_pes])
    return matches_5


def matchkey_6(CEN, PES, relationship_cen='relationship_hh_cen', relationship_pes='relationship_hh_pes',
               id_cen='EAid_cen', id_pes='EAid_pes'):
    """
    Matchkey 6: Allowing last name to be different
    """
    matches_6 = pd.merge(left=CEN,
                         right=PES,
                         how="inner",
                         left_on=['first_name_cen', 'year_cen', 'sex_cen', relationship_cen, id_cen],
                         right_on=['first_name_pes', 'year_pes', 'sex_pes', relationship_pes, id_pes])
    return matches_6


def matchkey_7(CEN, PES, relationship_cen='relationship_hh_cen', relationship_pes='relationship_hh_pes',
               id_cen='EAid_cen', id_pes='EAid_pes'):
    """
    Matchkey 7: Allowing first name to be different
    """

    matches_7 = pd.merge(left=CEN,
                         right=PES,
                         how="inner",
                         left_on=['last_name_cen', 'year_cen', 'sex_cen', relationship_cen, id_cen],
                         right_on=['last_name_pes', 'year_pes', 'sex_pes', relationship_pes, id_pes])
    return matches_7


def matchkey_8(CEN, PES, relationship_cen='relationship_hh_cen', relationship_pes='relationship_hh_pes',
               id_cen='EAid_cen', id_pes='EAid_pes'):
    """
    Matchkey 8: Swapped names
    """
    matches_8 = pd.merge(left=CEN,
                         right=PES,
                         how="inner",
                         left_on=['last_name_cen', 'first_name_cen', 'year_cen', 'sex_cen', relationship_cen,
                                  id_cen],
                         right_on=['first_name_pes', 'last_name_pes', 'year_pes', 'sex_pes', relationship_pes,
                                   id_pes])
    return matches_8


def matchkey_9(CEN, PES, edit_distance_limit=2, relationship_cen='relationship_hh_cen',
               relationship_pes='relationship_hh_pes', id_cen='EAid_cen', id_pes='EAid_pes'):
    """
    Matchkey 9: Swapped names with a small amount of error
    """

    matches_9 = pd.merge(left=CEN,
                         right=PES,
                         how="inner",
                         left_on=['year_cen', 'sex_cen', relationship_cen, id_cen],
                         right_on=['year_pes', 'sex_pes', relationship_pes, id_pes])
    matches_9['EDIT1'] = matches_9[['first_name_pes', 'last_name_cen']].apply(
        lambda x: jellyfish.levenshtein_distance(str(x[0]), str(x[1])), axis=1)
    matches_9['EDIT2'] = matches_9[['last_name_pes', 'first_name_cen']].apply(
        lambda x: jellyfish.levenshtein_distance(str(x[0]), str(x[1])), axis=1)
    matches_9 = matches_9[(matches_9.EDIT1 < edit_distance_limit) | (matches_9.EDIT2 < edit_distance_limit)]

    return matches_9


def matchkey_10(CEN, PES, edit_distance_limit=2, id_cen='DSid_cen', id_pes='DSid_pes'):
    """
    Matchkey 2: Edit Distance < 2 + Year + Month + District/EA/HH
    """
    matches_2 = pd.merge(left=CEN,
                         right=PES,
                         how="inner",
                         left_on=['year_cen', 'month_cen', id_cen],
                         right_on=['year_pes', 'month_pes', id_pes])
    matches_2['EDIT'] = matches_2[['fullnm_cen', 'fullnm_pes']].apply(
        lambda x: jellyfish.levenshtein_distance(str(x[0]), str(x[1])), axis=1)
    matches_2 = matches_2[matches_2.EDIT < edit_distance_limit]
    return matches_2
