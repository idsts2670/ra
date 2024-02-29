#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb  8 16:10:42 2024

@author: dwchuang-mbpi
"""

from sec_api import RenderApi    

import pandas as pd

# load Russell 3000 holdings CSV into a dataframe
holdings = pd.read_csv('./mikecik_2015.csv')
holdings.head(10)

# create batches of tickers: [[A,B,C], [D,E,F], ...]
# a single batch has a maximum of max_length_of_batch tickers
def create_batches(tickers = [], max_length_of_batch = 100):
  batches = [[]]

  for ticker in tickers:
    if len(batches[len(batches)-1]) == max_length_of_batch:
      batches.append([])

    batches[len(batches)-1].append(ticker)

  return batches


batches = create_batches(list(holdings['Ticker']))





from sec_api import QueryApi, RenderApi
from pathlib import Path
import multiprocessing

api_key='cfa0929a6e4b23d8a7f882039072195652578fdcf7cc0ee9732ad712ff9834f1'

queryApi = QueryApi(api_key=api_key)

def download_10K_metadata(tickers = [], start_year = 2015, end_year = 2015):
  if Path('metadata.csv').is_file():
    result = pd.read_csv('metadata.csv')
    return result

  print('Starting download process')

  # create ticker batches, with 100 tickers per batch
  batches = create_batches(tickers)
  frames = []

  for year in range(start_year, end_year + 1):
    for batch in batches:
      tickers_joined = ', '.join(batch)
      ticker_query = 'ticker:({})'.format(tickers_joined)

      query_string = '{ticker_query} AND filedAt:[{start_year}-01-01 TO {end_year}-12-31] AND formType:"10-K" AND NOT formType:"10-K/A" AND NOT formType:NT'.format(ticker_query=ticker_query, start_year=year, end_year=year)

      query = {
        "query": { "query_string": { 
            "query": query_string,
            "time_zone": "America/New_York"
        } },
        "from": "0",
        "size": "200",
        "sort": [{ "filedAt": { "order": "desc" } }]
      }

      response = queryApi.get_filings(query)

      filings = response['filings']

      metadata = list(map(lambda f: {'ticker': f['ticker'], 
                                     'cik': f['cik'], 
                                     'formType': f['formType'], 
                                     'filedAt': f['filedAt'], 
                                     'filingUrl': f['linkToFilingDetails']}, filings))

      df = pd.DataFrame.from_records(metadata)

      frames.append(df)

    print('Downloaded metadata for year', year)


  result = pd.concat(frames)
  result.to_csv('metadata.csv', index=False)

  number_metadata_downloaded = len(result)
  print('Downloaded completed. Metadata downloaded for {} filings.'.format(number_metadata_downloaded))

  return result


tickers = list(holdings['Ticker'])

metadata = download_10K_metadata(tickers=tickers, start_year=2015, end_year=2015)


import requests
import datetime
API_ENDPOINT = "https://api.sec-api.io/filing-reader"
API_KEY = "458301555a0838acf5e8eca62a944cce9186d6612e35a3fb5e8fcfefef7fbf5cff"
filing_url = metadata['filingUrl'].replace('ix?doc=/', '')
api_url = API_ENDPOINT + "?token=" + API_KEY + "&url=" + filing_url + "&type=pdf"
file_name = filing_url.str.rsplit("/", n=1, expand=True)[1]
file_name_cik = metadata['cik']


for i in range(0, len(metadata)):
    response = requests.get(api_url.iloc[i])
    # save the PDF to a local file "filing.pdf"

    with open(file_name_cik.iloc[i] + file_name.iloc[i]+".pdf", 'wb') as f:
        f.write(response.content)