# PREP
import pandas as pd
import numpy as np
# import polars as pl
import matplotlib.pyplot as plt
import re
import os
import glob
import shutil
from pathlib import Path
from datetime import datetime, timedelta
os.getcwd()

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

# Polars function to let us read csv files without having to specify the directory
# def read_csvpl(name, **kwrgs):
#     path = os.path.join(INPUT_DIR, name + '.csv')
#     print(f'Load: {path}')
#     return pl.read_csv(path, **kwrgs)


# data import
ma_with_patent = read_csv("roster3-csv-corrected-20230911")


# preprocessing
# select only rows with non-null Withdrawn date and with valid Withdrawn date
tmp1 = ma_with_patent[~ma_with_patent["DateWithdrawn"].isna()].loc[ma_with_patent["DateWithdrawn"] != "-"]

# change the data type of "DateWithdrawn" to datetime
tmp1["Date"] = pd.to_datetime(tmp1["DateWithdrawn"])
# make the index as a column (save the id?)
# tmp1.reset_index(inplace=True)

# select only rows with non-null realized date and with valid realized date
tmp2 = ma_with_patent[~ma_with_patent["DateEffectiveUnconditional"].isna()].loc[ma_with_patent["DateEffectiveUnconditional"] != "-"]

# change the data type of "DateEffectiveUnconditional" to datetime
tmp2["Date"] = pd.to_datetime(tmp2["DateEffectiveUnconditional"])

# extract only the rows with both acq_sic and tgt_sic3, meaning acquiror and target are both from the same industry
# tmp2 = tmp2[~tmp2["acq_sic3"].isna() & ~tmp2["tgt_sic3"].isna()].query('acq_sic3 == tgt_sic3')

# Concatenate the tmp1 and tmp2
tmp3 = pd.DataFrame(pd.concat([tmp1, tmp2]))
# if we want to handle id as character, run the code below.
# tmp3 = tmp3.astype({"id":"string"})



# Create the 36-month window
# Filter rows where DateWithdrawn is not null
filtered_tmp3 = tmp3[tmp3["DateWithdrawn"] != "-"]

# Create a dictionary to store the resulting data
result_dict = {}

# Iterate over each row in the filtered dataframe
for index, row in filtered_tmp3.iterrows():
    base_date = row["Date"]
    base_id = row["id"]

    # Calculate the 36-month window
    start_date = base_date - timedelta(days=365 * 1.5)
    end_date = base_date + timedelta(days=365 * 1.5)

    # Filter rows where DateWithdrawn is missing and date falls within the window
    temp_df = tmp3[(tmp3["DateWithdrawn"] == "-") & (tmp3["Date"] >= start_date) & (tmp3["Date"] <= end_date)]["id"]

    # Convert the filtered rows to a list and append it to the result_lists
    temp_list = temp_df.values.tolist()

    # Add the key-value pair to the result_dict
    result_dict[base_id] = temp_list

# Convert the dictionary to a DataFrame
## converts each key-value pair in the original dictionary into a Series. \
## This allows the DataFrame to be built in such a way that missing values (since lists can be of different lengths) are filled with NaN.
result_df = pd.DataFrame(dict([(k,pd.Series(v)) for k,v in result_dict.items()])).transpose().reset_index().rename(columns={"index": "withdrawn_m&a_id"})

# transpose the dataframe and set the first row as the column names
result_df = result_df.transpose()
result_df.columns = result_df.iloc[0]
result_df = result_df[1:]

# Reshape the DataFrame into a long format using melt()
result_df = result_df.melt(var_name='column', value_name='value')

# Set the index to the column names
result_df = result_df.set_index('column')['value'].to_frame()
result_df = result_df.reset_index().rename(columns={"column": "withdrawn_m&a_id", "value": "realized_m&a_id"})

# merge the result with the original tmp3
result_df = pd.merge(result_df, tmp3, left_on=["realized_m&a_id"], right_on = ["id"], how = "inner")

# select only the columns we need
result_df = result_df[["withdrawn_m&a_id", "realized_m&a_id", "AcquirorName", "TargetName", "AcquirorMacroIndustry", "TargetMacroIndustry", "acq_sic", "tgt_sic"]]

# export to csv file
result_df.to_csv(os.path.join(OUTPUT_DIR, "MAtemp-conTroLgroupfor_withdrwnMApairsrstep1_36monthwindow.csv"), encoding='utf_8_sig', index=False)




# create the 24-month window
# Filter rows where DateWithdrawn is not null
filtered_tmp3 = tmp3[tmp3["DateWithdrawn"] != "-"]

# Create a dictionary to store the resulting data
result_dict = {}

# Iterate over each row in the filtered dataframe
for index, row in filtered_tmp3.iterrows():
    base_date = row["Date"]
    base_id = row["id"]

    # Calculate the 2 years window
    start_date = base_date - timedelta(days=365)
    end_date = base_date + timedelta(days=365)

    # Filter rows where DateWithdrawn is missing and date falls within the window
    temp_df = tmp3[(tmp3["DateWithdrawn"] == "-") & (tmp3["Date"] >= start_date) & (tmp3["Date"] <= end_date)]["id"]

    # Convert the filtered rows to a list and append it to the result_lists
    temp_list = temp_df.values.tolist()

    # Add the key-value pair to the result_dict
    result_dict[base_id] = temp_list

# Convert the dictionary to a DataFrame
## converts each key-value pair in the original dictionary into a Series. \
## This allows the DataFrame to be built in such a way that missing values (since lists can be of different lengths) are filled with NaN.
result_df = pd.DataFrame(dict([(k,pd.Series(v)) for k,v in result_dict.items()])).transpose().reset_index().rename(columns={"index": "withdrawn_m&a_id"})

# transpose the dataframe and set the first row as the column names
result_df = result_df.transpose()
result_df.columns = result_df.iloc[0]
result_df = result_df[1:]

# Reshape the DataFrame into a long format using melt()
result_df = result_df.melt(var_name='column', value_name='value')

# Set the index to the column names
result_df = result_df.set_index('column')['value'].to_frame()
result_df = result_df.reset_index().rename(columns={"column": "withdrawn_m&a_id", "value": "realized_m&a_id"})

# merge the result with the original tmp3
result_df = pd.merge(result_df, tmp3, left_on=["realized_m&a_id"], right_on = ["id"], how = "inner")

# select only the columns we need
result_df = result_df[["withdrawn_m&a_id", "realized_m&a_id", "AcquirorName", "TargetName", "AcquirorMacroIndustry", "TargetMacroIndustry", "acq_sic", "tgt_sic"]]

# export to csv file
result_df.to_csv(os.path.join(OUTPUT_DIR, "MAtemp-conTroLgroupfor_withdrwnMApairsrstep1_24monthwindow.csv"), encoding='utf_8_sig', index=False)




# create the 4 years window
# Filter rows where DateWithdrawn is not null
filtered_tmp3 = tmp3[tmp3["DateWithdrawn"] != "-"]

# Create a dictionary to store the resulting data
result_dict = {}

# Iterate over each row in the filtered dataframe
for index, row in filtered_tmp3.iterrows():
    base_date = row["Date"]
    base_id = row["id"]

    # Calculate the 4 years window
    start_date = base_date - timedelta(days=365 * 2)
    end_date = base_date + timedelta(days=365 * 2)

    # Filter rows where DateWithdrawn is missing and date falls within the window
    temp_df = tmp3[(tmp3["DateWithdrawn"] == "-") & (tmp3["Date"] >= start_date) & (tmp3["Date"] <= end_date)]["id"]

    # Convert the filtered rows to a list and append it to the result_lists
    temp_list = temp_df.values.tolist()

    # Add the key-value pair to the result_dict
    result_dict[base_id] = temp_list

# Convert the dictionary to a DataFrame
## converts each key-value pair in the original dictionary into a Series. \
## This allows the DataFrame to be built in such a way that missing values (since lists can be of different lengths) are filled with NaN.
result_df = pd.DataFrame(dict([(k,pd.Series(v)) for k,v in result_dict.items()])).transpose().reset_index().rename(columns={"index": "withdrawn_m&a_index"})

# transpose the dataframe and set the first row as the column names
result_df = result_df.transpose()
result_df.columns = result_df.iloc[0]
result_df = result_df[1:]

# Reshape the DataFrame into a long format using melt()
result_df = result_df.melt(var_name='column', value_name='value')

# Set the index to the column names
result_df = result_df.set_index('column')['value'].to_frame()
result_df = result_df.reset_index().rename(columns={"column": "withdrawn_m&a_id", "value": "realized_m&a_id"})

# merge the result with the original tmp3
result_df = pd.merge(result_df, tmp3, left_on=["realized_m&a_id"], right_on = ["id"], how = "inner")

# select only the columns we need
result_df = result_df[["withdrawn_m&a_id", "realized_m&a_id", "AcquirorName", "TargetName", "AcquirorMacroIndustry", "TargetMacroIndustry", "acq_sic", "tgt_sic"]]

# export to csv file
result_df.to_csv(os.path.join(OUTPUT_DIR, "MAtemp-controlgroupfor_withdrwnMApairsrstep1_48monthwindow.csv"), encoding='utf_8_sig', index=False)




# Create the 5 years window
# Filter rows where DateWithdrawn is not null
filtered_tmp3 = tmp3[tmp3["DateWithdrawn"] != "-"]

# Create a dictionary to store the resulting data
result_dict = {}

# Iterate over each row in the filtered dataframe
for index, row in filtered_tmp3.iterrows():
    base_date = row["Date"]
    base_id = row["id"]

    # Calculate the 5 years window
    start_date = base_date - timedelta(days=365 * 2.5)
    end_date = base_date + timedelta(days=365 * 2.5)

    # Filter rows where DateWithdrawn is missing and date falls within the window
    temp_df = tmp3[(tmp3["DateWithdrawn"] == "-") & (tmp3["Date"] >= start_date) & (tmp3["Date"] <= end_date)]["id"]

    # Convert the filtered rows to a list and append it to the result_lists
    temp_list = temp_df.values.tolist()

    # Add the key-value pair to the result_dict
    result_dict[base_id] = temp_list

# Convert the dictionary to a DataFrame
## converts each key-value pair in the original dictionary into a Series. \
## This allows the DataFrame to be built in such a way that missing values (since lists can be of different lengths) are filled with NaN.
result_df = pd.DataFrame(dict([(k,pd.Series(v)) for k,v in result_dict.items()])).transpose().reset_index().rename(columns={"index": "withdrawn_m&a_index"})

# transpose the dataframe and set the first row as the column names
result_df = result_df.transpose()
result_df.columns = result_df.iloc[0]
result_df = result_df[1:]

# Reshape the DataFrame into a long format using melt()
result_df = result_df.melt(var_name='column', value_name='value')

# Set the index to the column names
result_df = result_df.set_index('column')['value'].to_frame()
result_df = result_df.reset_index().rename(columns={"column": "withdrawn_m&a_id", "value": "realized_m&a_id"})

# merge the result with the original tmp3
result_df = pd.merge(result_df, tmp3, left_on=["realized_m&a_id"], right_on = ["id"], how = "inner")

# select only the columns we need
result_df = result_df[["withdrawn_m&a_id", "realized_m&a_id", "AcquirorName", "TargetName", "AcquirorMacroIndustry", "TargetMacroIndustry", "acq_sic", "tgt_sic"]]

# export to csv file
result_df.to_csv(os.path.join(OUTPUT_DIR, "MAtemp-controlgroupfor_withdrwnMApairsrstep1_60 monthwindow.csv"), encoding='utf_8_sig', index=False)