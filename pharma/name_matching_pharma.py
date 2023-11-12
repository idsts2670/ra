# %%
import pandas as pd
import numpy as np
import polars as pl
import matplotlib.pyplot as plt
import re
import os
import glob
import shutil
import openpyxl
from openpyxl import Workbook
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from pathlib import Path
from datetime import datetime, timedelta
os.getcwd()
pd.options.display.max_rows = 1000

# %%
# create the input_dir（input directory）
source_path = os.path.dirname(os.path.abspath('__file__'))
# source_path = '/scratch/bell/sido/m&a'
INPUT_DIR = os.path.join(source_path, 'data')

# if INPUT_DIR has not been created yet, create it
if not os.path.isdir(INPUT_DIR):
    os.mkdir(INPUT_DIR)

# output_dir(output directory) creation
OUTPUT_DIR = os.path.join(source_path, 'outputs')

# if OUTPUT_DIR has not been created yet, create it
if not os.path.isdir(OUTPUT_DIR):
    os.mkdir(OUTPUT_DIR)

# %%
# Once you run this code, comment it out
# move csv files to `data` directory(=folder)

# unique_dir_names = []
# for f in Path(f'{source_path}').rglob('*.csv'):
#     unique_dir_names.append(f)
# for g in Path(f'{source_path}').rglob('*.xlsx'):
#     unique_dir_names.append(g)

# for file in list(set(unique_dir_names)):
#     print(f'moved file: {file}')
#     shutil.move(f'{file}', f'{INPUT_DIR}')


# Pandas function to let us read csv files without having to specify the directory
def read_csv(name, **kwrgs):
    path = os.path.join(INPUT_DIR, name + '.csv')
    print(f'Load: {path}')
    return pd.read_csv(path, **kwrgs)

# Polars function to let us read csv files wi`thout having to specify the directory
def read_csvpl(name, **kwrgs):
    path = os.path.join(INPUT_DIR, name + '.csv')
    print(f'Load: {path}')
    return pl.read_csv(path, **kwrgs)


# DATA IMPORT

#convert xlsx to csv
# firm_dir = pd.DataFrame(pd.read_excel(os.path.join(INPUT_DIR, "USPTO_firm_list.xlsx")))
# firm_dir.to_csv(os.path.join(INPUT_DIR, "uspto_firm_list.csv"), encoding='utf-8', index=False)
# temproster = pd.DataFrame(pd.read_excel(os.path.join(INPUT_DIR, "temproster2_allversion_targetTOmissingand_recapdatanotmissingOct2023_Firmlist.xlsx")))
# temproster.to_csv(os.path.join(INPUT_DIR, "temproster2_allversion_targetTOmissingand_recapdatanotmissingOct2023_Firmlist.csv"), encoding='utf-8', index=False)

firm_dir = read_csv('uspto_firm_list')
temproster = read_csv('temproster2_allversion_targetTOmissingand_recapdatanotmissingOct2023_Firmlist')


# PREPROCESS
# Check for non-string or missing values in the 'organization' column
firm_dir[firm_dir['organization'].apply(lambda x: not isinstance(x, str))]

# fill NaN values with an empty string
firm_dir['organization'].fillna("", inplace=True)

# regular expression to remove the following words from the company name
def clean_company_name(name):
    # extend the regular expression to include 'A/S' and use a non-capturing group for postfixes
    # the regex will handle cases like "Co. Ltd." to remove following words    
    postfix_pattern = re.compile(
        r'(,?\b\s*(Inc(?=\W|$)\.?|Ltd(?=\W|$)\.?|LLC|LTD|GmbH|\& Co\.?\s*|\bCo\b\s*|\bCo\.\s*|Corp|Corporation|S\.A\.|S\.P\.A\.|S\.A\.S\.|S\.R\.L\.|LLP|LP|'
        r'S\.L\.|Oyj|Zrt|Pty|Kg|Kgaa|N\.V\.|HF|BVBA|B\.V\.|S\.E\.|PLC|PBC|S\.C\.A\.|S\.E\.M\.C\.O\.|S\.E\.C\.|'
        r'Limited).*)', re.IGNORECASE)

    # substitute found patterns with an empty string
    cleaned_name = re.sub(postfix_pattern, "", name).strip()
    cleaned_name = cleaned_name.lower()
    
    return cleaned_name


# apply the updated function to the dataframe
firm_dir["cleaned_organization"] = firm_dir["organization"].apply(clean_company_name)
temproster["cleaned_TargetName"] = temproster["TargetName"].apply(clean_company_name)

temproster.to_csv(os.path.join(INPUT_DIR, "temproster.csv"), encoding='utf-8', index=False)
firm_dir.to_csv(os.path.join(INPUT_DIR, "firm_dir.csv"), encoding='utf-8', index=False)


# MATCHING
# rename the new column to 'name'
temproster2 = temproster.rename(columns={"cleaned_TargetName": "name"})

## Exact matching
# simple merge
merged_df = pd.merge(temproster2, firm_dir, left_on="name", right_on="cleaned_organization", how="left")

# rename the new column to 'name' and drop the data from firm_dir that was merged if it was not a match
merged_df2 = merged_df.rename(columns={"name": "cleaned_TargetName"}).dropna(subset=["cleaned_organization"])[["TargetName", "cleaned_TargetName", "organization", "cleaned_organization", "assignee_id"]]
merged_df2.head(20)

merged_df2.to_csv(os.path.join(OUTPUT_DIR, "merged_df.csv"), encoding='utf-8', index=False)
merged_df2.to_excel(os.path.join(OUTPUT_DIR, "merged_df.xlsx"), index=False)



# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Ignore below ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## Fuzzy matching
# define a function to get the best match from the list of company names in company_dir for companies names in temproster
def get_fuzzy_match(row, choices, scorer, threshold):
    all_matches = process.extractOne(row["name"], choices=choices, scorer=scorer)
    print(all_matches)
    # Filter matches based on the threshold
    # valid_matches = [match for match, score in all_matches if score >= threshold]
    return print(all_matches)

# find all matches for each company name in temproster
matches = []
for i, row in temproster2.iterrows():
    get_fuzzy_match(row, firm_dir["cleaned_organization"].tolist(), scorer=fuzz.WRatio, threshold=98)


# define a function to get the best match from the list of company names in company_dir for companies names in temproster
def get_fuzzy_match(row, choices, scorer, threshold):
    all_matches = process.extractOne(row["name"], choices=choices, scorer=scorer)
    # Filter matches based on the threshold
    valid_matches = [match for match, score in all_matches if score >= threshold]
    return valid_matches

# find all matches for each company name in temproster
matches = []
for i, row in temproster2.iterrows():
    match_list = get_fuzzy_match(row, firm_dir["cleaned_organization"].tolist(), scorer=fuzz.WRatio, threshold=98)
    for match in match_list: 
        matches.append({"temproster_TargetName": row["name"], "firm_dir_organization": match, "firm_dir_assignee_id": firm_dir[firm_dir["cleaned_organization"] == match]["assignee_id"].values[0]})

# convert the list of match dictionaries to a DataFrame
matches_df = pd.DataFrame(matches)