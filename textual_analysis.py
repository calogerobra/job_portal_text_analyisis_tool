# -*- coding: utf-8 -*-
"""
In this script, we construct a search engine to conduct a text search in
vacancy data.
"""
# Import necessary packages
import pandas as pd
import numpy as np
import time
import string

def read_stata_data(input_path, filename):
    """ Import Stata file and retrun dataframe.
    Args:
        input_path: Path where cleaned Stata .dta file lies
        filename: filename of Stata .dta file
    Returns:
        dataframe
    """
    file = input_path + filename
    return pd.read_stata(file, encoding = "latin-1" )

def read_excel_data(input_path, filename, sheet):
    """ Import raw vacanciy data from Excel file.
    Args:
        input_path: Raw data location
        filename: Raw data fiel name (.xlsx)
        sheet_name: Sheet name of data
    Returns:
        pandas dataframe
    """
    return pd.read_excel(input_path + filename, sheet_name  = sheet, converters={'nace_code':str}).drop_duplicates()

def write_excel_data(dataframe, output_path, output_filename, sheet_name):
    """ Import raw vacanciy data from Excel file.
    Args:
        input_path: Raw data location
        output_filename: Raw data fiel name (.xlsx)
        sheet_name: Sheet name of data
    Returns:
        pandas dataframe
    """
    return dataframe.to_excel(output_path + output_filename , sheet_name = sheet_name, index = False)

def unify_job_description(dataframe, colname1, colname2, newcol):
    """ Migrate job description and imputed job_description
    Args:
        colname1: Column with imputed
        colname2: Column with old job description
        newcol: New harmonized column
    Returns:
        Dataframe
    """
    dataframe[[colname1, colname2]] = dataframe[[colname1,colname2]].fillna(value='')
    dataframe[newcol] = dataframe[colname1].astype(str) + "  " + dataframe[colname2].astype(str)
    dataframe[newcol + "_strict"] = ""
    dataframe.loc[(dataframe.jobdesc_imputation == 0) | (dataframe.jobdesc_imputation == 1), newcol + "_strict"]  = dataframe[colname1].astype(str) + "  " + dataframe[colname2].astype(str)
    return dataframe

def make_lowercase(dataframe, col_name):
    """ Transform string to lower case string
    Args:
        dataframe: Stata generaated pandas dataframe
        col_name: Column name containing job description
    Returns:
        cleaned dataframe
    """
    col_name_mod = col_name + "_old"
    dataframe[col_name_mod] = dataframe[col_name]
    dataframe[col_name] = dataframe[col_name].str.lower()
    return dataframe

def remove_punctuation(dataframe, col_name, rep_char):
    """ Remove punctuation by rep_char
    Args:
        rep_char: String to be replaced in
    Returns:
        Cleaned dataframe
    """
    num_list = [str(num) for num in range(0,100)]
    punc_list = list(string.punctuation)
    for i in ['+', '#']:
              punc_list.remove(i)
    tot_list = num_list + punc_list
    for punc in tot_list:
        dataframe[col_name] = dataframe[col_name].str.replace(punc, rep_char)
    return dataframe


def clean_spec_chars(cleaning_path, cleaning_filename, spec_char_sheet, dataframe, col_name):
    """Clean text from special characters to facilitate term frequency generation.
    Args:
        dataframe: Stata generaated pandas dataframe
        col_name: Column name containing job description
        stop_word_dict: Dictionary of stop words in different languages
        spec_char_dict: Dictionary of special characters in different languages
        spec_word_dict: Dictionary of specific words in different languages
    Returns:
        cleaned dataframe
    """
    cleaner = read_excel_data(cleaning_path, cleaning_filename, spec_char_sheet)
    languages = list(cleaner)
    for ln in languages:
        to_cancel = cleaner[ln].dropna().tolist()
        for item in to_cancel:
            item = item.split(", ")
            old = item[0]
            new = item[1]
            dataframe[col_name] = dataframe[col_name].str.replace(old, new)
    return dataframe

def clean_stop_words(cleaning_path, cleaning_filename, stop_word_sheet, dataframe, col_name):
    """Clean text from stopwords to facilitate term frequency generation.
    Args:
        cleaning_file: Cleaning .xlsx file
        cleaning_sheet: Sheet name with stop word list
        dataframe: Stata generate pandas dataframe with job description
        col_name: Column name containing job description
    Returns:
        cleaned vacancy column
    """
    cleaner = read_excel_data(cleaning_path, cleaning_filename, stop_word_sheet)
    languages = list(cleaner)
    for ln in languages:
        to_cancel = cleaner[ln].dropna().tolist()
        for item in to_cancel:
            item =  " " + item + " "
            dataframe[col_name] = dataframe[col_name].str.replace(item," ")
    return dataframe

def clean_others(cleaning_path, cleaning_filename, others_sheet, dataframe, col_name):
    """Clean text from other special words and characters to
    facilitate term frequency generation.
    Args:
        dataframe: Stata generaated pandas dataframe
        col_name: Column name containing job description
        stop_word_dict: Dictionary of stop words in different languages
        spec_char_dict: Dictionary of special characters in different languages
        spec_word_dict: Dictionary of specific words in different languages
    Returns:
        cleaned vacancy column
    """
    cleaner = read_excel_data(cleaning_path, cleaning_filename, others_sheet)
    languages = list(cleaner)
    for ln in languages:
        to_cancel = cleaner[ln].dropna().tolist()
        for item in to_cancel:
            item =  " " + str(item) + " "
            dataframe[col_name] = dataframe[col_name].str.replace(item," ")
    return dataframe

def identify_regex_keys(key_str, regex_dict):
    """ Identify if the key is a regular expression or a simple key word in order
    to allow for differential treatment. If a regex key is found we want to build
    the corresponding regular expression, otherwise just pass the key word
    to be made lowercase.
    Args:
        key_str: String input from key list
        regex_dict: dictionary of possible regex conditions
    Returns:
        Raw string or transformed regex condition
    NOTE: This function needs to be extended in case more than type 1 regex will
    be added.
    """
    if "[" and "]" in key_str:
        # 1. check length to esure it's a type 1 expression
        wordlist = key_str.split(';')[0].replace("[","").replace("]","").split(',')
        nwords = key_str.split(';')[1].replace("(","").replace(")","").split(',')
        # If standard case choose type 1 regex from dict
        if len(wordlist) == 2:
            regex_raw = regex_dict[1] # check index from input dictionary
            regex_str = regex_raw.replace('{a', '{' + nwords[0])
            regex_str = regex_str.replace('b}', nwords[1] + '}')
            regex_str = regex_str.replace('word1', wordlist[0])
            regex_str = regex_str.replace('word2', wordlist[1])
            return regex_str
        elif len(wordlist) == 3:
            regex_raw = regex_dict[2] # check index from input dictionary
            regex_str = regex_raw.replace('{a', '{' + nwords[0])
            regex_str = regex_str.replace('b}', nwords[1] + '}')
            regex_str = regex_str.replace('word1', wordlist[0])
            regex_str = regex_str.replace('word2', wordlist[1])
            regex_str = regex_str.replace('word3', wordlist[2])
            return regex_str
        else:
            raise TypeError('Check regex conditions!')
    else:
        return key_str

def read_dictionary(dictionary_excel_path, dictionary_excel_file, cleaning_path, cleaning_filename, spec_char_sheet):
    """ Reads dictionary xlsx file and stores single skills sheets as seperate
    dataframes.
    Args:
        dictionary_excel_path: Path to dictionary file
        dictionary_excel_file: Dictionary fiename
    Retruns:
        list of dataframes-name tuples
    """
    frames = []
    file = dictionary_excel_path + dictionary_excel_file
    dictionary = pd.read_excel(file, sheet_name = None)
    for sheet in dictionary:
        name = sheet
        df = dictionary[sheet]
        # Erase sepcial characters in each column and make lower case
        cols = df.columns.values.tolist()
        try:
            for col in cols:
                df = clean_spec_chars(cleaning_path, cleaning_filename, spec_char_sheet, df, col)
                df = make_lowercase(df, col)
        except AttributeError:
            pass
        frames.append((name, df))
    return frames

def build_search_dict_list(dictionary_list, regex_dict):
    """ Build a list of dictionaries, where each dictionary corresponds to
    one sheet in the dictionary excel file and its keys are requirements and their
    values ar ea list of key words
    Args:
        dictionary_list
    Retrurns:
        List of pyhon (skill name, dictionary)-tuples
    """
    skill_dictionaries = []
    for element in dictionary_list:
        skillname = element[0]
        skill = element[1]
        requirements = list(set(skill.requirement.values.tolist()))
        skill_dictionary = {}
        for req in requirements:
            subframe = skill[skill.requirement == req]
            # Exclude first clumn and old columns
            cols = subframe.columns.tolist()
            for colname in ['requirement', 'requirement_old', 'key_en_old',
                             'key_sq_old', 'key_de_old' ]:
                try:
                    cols.remove(colname)
                except ValueError:
                    continue
            keys = []
            for col in cols:
                keys = keys + subframe[col].values.tolist()
            # Make unique
            keys =  [i for i in list(set(keys)) if str(i) != 'nan']
            # Replace regex expression if needed
            #print(keys)
            keys = [identify_regex_keys(i, regex_dict) for i in keys] # Check this
            # Update single skill category dictionary
            skill_dictionary.update(dict([
                    (req, keys)
                    ]))
        comb = (skillname, skill_dictionary)
        skill_dictionaries.append(comb)
    return skill_dictionaries

def build_search_matrix(dataframe, pydict_list, search_col):
    """ Construct columns with the search results for each vacancy containing
    a dummy if a skill is fulfilled whenever a requirement is fulfilled and
    attaching a string of ordered key words to see which key words were found in
    the respective description
    Args:
        dataframe: Dataset for search
        pydict_list: List of search python dicts
        search_col: column name where to search for the respective key words
    Returns:
        Dataframe with search results
    """
    for skill in pydict_list:
        skill_dictionary = skill[1]
        skillname = skill[0]
        print("Searching for skill:", skillname, "...")
        # Create column with skill, and placeholder for requirements and keys
        dataframe["s_" + skillname] = ""
        dataframe["req_" + skillname ] = np.empty((len(dataframe), 0)).tolist()
        dataframe["keys_" + skillname] = np.empty((len(dataframe), 0)).tolist()
        #loop over requirements
        for req in skill_dictionary:
            # Extract list of key words to be searched
            key_list = skill_dictionary[req]
            # Search for each key word
            for key in key_list:
                # Store temporary column for current key word and make case distinction for regular erxpressions
                if all(x in key for x in ['w+', 'W+', '(?:\w+\W+)']):
                    dataframe["current_key"] = dataframe[search_col].str.contains(key) # Note since we include reguular expressions regex = True over here!!
                else:
                    dataframe["current_key"] = dataframe[search_col].str.contains(key, regex = False)
                # If key word is contained add skill requirement and key word tp seperate list
                dataframe.loc[dataframe.current_key == True, "req_" + skillname ].apply(lambda x: x.append(str(req)))
                dataframe.loc[dataframe.current_key == True, "keys_" + skillname].apply(lambda x: x.append(str(key)))
            # Erase duplicates
            dataframe["req_" + skillname ] = dataframe["req_" + skillname ].apply(set).apply(list)
            dataframe["keys_" + skillname] = dataframe["keys_" + skillname].apply(set).apply(list)
        # Create "|"-concatenated string
        dataframe["req_" + skillname ] = dataframe["req_" + skillname ].str.join("|")
        dataframe["keys_" + skillname] = dataframe["keys_" + skillname].str.join("|")
        # If skills and requirement list are not empty, skill is containes, hence input 1, else 0
        dataframe.loc[dataframe["req_" + skillname] != "", "s_" + skillname] = 1
        dataframe.loc[dataframe["req_" + skillname] == "", "s_" + skillname] = 0
        dataframe = dataframe.drop(columns=["current_key"])
    return dataframe

def main():
    # Set input parameters
    input_path = "C:\\Users\\Calogero\\Documents\\GitHub\\job_portal_text_analyisis_tool\\data\\input\\"
    cleaning_path = "C:\\Users\\Calogero\\Documents\\GitHub\\job_portal_text_analyisis_tool\\data\\input\\"
    filename = "sample_jp_data.xlsx"
    cleaning_filename = "cleaning_import.xlsx"

    output_path = "C:\\Users\\Calogero\\Documents\\GitHub\\job_portal_text_analyisis_tool\\data\\output\\"
    output_filename = "sample_jp_data_out.xlsx"
    output_sheet = "text_search"
    # Run algortihm
    start_time = time.time() # Capture start and end time for performance

    print("Reading and pre-processing data...")
    dataframe = read_excel_data(input_path, filename, "sample")
    #dataframe = unify_job_description(dataframe, "job_description", "job_description_adj", "ta_str")
    dataframe = make_lowercase(dataframe, "job_description")
    dataframe = clean_stop_words(cleaning_path, cleaning_filename, "stop_words", dataframe, "job_description")
    dataframe = remove_punctuation(dataframe, "job_description", " ")
    dataframe = clean_spec_chars(cleaning_path, cleaning_filename, "special_characters", dataframe, "job_description")
    dataframe = clean_others(cleaning_path, cleaning_filename, "others", dataframe, "job_description")

    # Include regular expression dictionary
    regex_dict = dict([(1, r'bword1W+(?:w+W+){a,b}?word2b'),
                        (2, r'bword1W+(?:w+W+){a,b}?word2bW+(?:w+W+){a,b}?word3b')])

    # Run text search
    dictionary_excel_path = "C:\\Users\\Calogero\\Documents\\GitHub\\job_portal_text_analyisis_tool\\data\\input\\"
    dictionary_excel_file = "dictionary.xlsx" # Insert most current file here
    dictionary_list = read_dictionary(dictionary_excel_path, dictionary_excel_file, cleaning_path, cleaning_filename, "special_characters")
    pydict_list = build_search_dict_list(dictionary_list, regex_dict)

    # Build search matrix
    search_matrix = build_search_matrix(dataframe, pydict_list, "job_description")
    write_excel_data(search_matrix, output_path, output_filename, output_sheet)

    end_time = time.time()
    duration = time.strftime("%H:%M:%S", time.gmtime(end_time - start_time))

    # For interaction and error handling
    final_text = "Your query was successful! Time elapsed:" + str(duration)
    print(final_text)

if __name__ == "__main__":
    main()
