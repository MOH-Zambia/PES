import pandas as pd
import datetime

def derive_names(df, fullname_column, suffix=""):
    """
    Derives first, middle and last names from a pandas dataframe column containing fullname
    :param df: input dataframe with fullname_column present
    :param fullname_column: name of column containing fullname as string type
    :param suffix: Optional suffix to append to name component column names
    :return: input dataframe with additional columns for first, second and last names
    """

    assert (df[fullname_column].dtype == str, fullname_column + " is not str type but %s." % df[fullname_column].dtype)

    # Derive first, second & last names
    df['first_name' + suffix] = df[fullname_column].str.split().str.get(0)
    df['second_name' + suffix] = df[fullname_column].str.split().str.get(1)
    df['last_name' + suffix] = df[fullname_column].str.split().str.get(-1)

    return df


def derive_dob(df, dob_column, seperator, suffix=""):
    """
    Derives day, month, year from pandas dataframe column containing full date of birth
    :param df: input dataframe with dob_column present
    :param dob_column: name of column containing string of full date of birth, preferably in format DD/MM/YYYY
    :param seperator: special character that splits DD MM and YYYY in dob_column
    :param suffix: Optional suffix to append to name component column names
    :return: input dataframe with additional columns for day, month, year
    """

    assert (df[dob_column].dtype == str, dob_column + " is not str type but %s." % df[dob_column].dtype)

    df['day' + suffix] = df[dob_column].str.split(pat=seperator).str.get(0)
    df['month' + suffix] = df[dob_column].str.split(pat=seperator).str.get(1)
    df['year' + suffix] = df[dob_column].str.split(pat=seperator).str.get(-1)

    return df


def derive_age(df, day_col, month_col, year_col, suffix="", census_date=datetime.date.today()):
    """
    Derives age using day, month and year columns with a defined census date
    :param df: input dataframe with required columns
    :param day_col: day of date of birth
    :param month_col: month of date of birth
    :param year_col: year of date of birth
    :param suffix: Optional suffix to append to name component column names
    :param census_date: date to calculate age at, in datetime format
    :return: dataframe with additional columns for dob in datetime and age in years
    """
    df['dob_datetime'+suffix] = df[[day_col, month_col,year_col]].apply(lambda x: '-'.join(x.values.astype(str)), axis='columns')
    df['dob_datetime'+suffix] = pd.to_datetime(df['dob_datetime'+suffix])
    df['age'+suffix] = (census_date - df['dob_datetime'+suffix]).astype('<m8[Y]')

    return df


def convert_cols_to_datatype(df, columns, datatype):
    """
    Converts pandas columns to specified datatype
    :param df: input dataframe with columns
    :param columns: list of column names
    :param datatype: datatype to convert columns to
    :return: dataframe with converted columns
    """

    df[columns] = df[columns].astype(datatype)

    return df
