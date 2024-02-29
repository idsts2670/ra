# %%
# %pip install sec-api
# %pip install pdfkit
# %pip install wkhtmltopdf

# %%
from sec_api import ExtractorApi
import pdfkit
import pandas as pd
import os
from sec_api import RenderApi
from pathlib import Path
import multiprocessing

# %%
os. getcwd()

# %%
# load Russell 3000 holdings CSV into a dataframe
holdings = pd.read_csv('./mikecik_2015/mikecik_2015.csv')
display(holdings.head())

# %%
# set api_key and QueryApi to search and filter filings later
from sec_api import QueryApi
api_key = "7caf4a7647d0fe6dd25bad53faa1028f2c47a4633a31b6d6d7c7f19a2ae8a575"
queryApi = QueryApi(api_key=api_key)

# %%
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


# %%
# Create metadata which contains ticker, cik, filedAt, and filingUrl

def download_10K_metadata(tickers = [], start_year = 2015, end_year = 2015):
  if Path('./mikecik_2015/metadata.csv').is_file():
    result = None
    result = pd.read_csv('./mikecik_2015/metadata.csv')
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

# %%
tickers = list(holdings['Ticker'])

# specify start year to end year
start = 2015
end = 2015

# create metadata
metadata = download_10K_metadata(tickers=tickers, start_year=start, end_year=end)
metadata.head()

# %%
# convert filedAt column to datetime format and extract year
metadata['filedAt'] = pd.to_datetime(metadata['filedAt'], utc=True).dt.tz_convert(None)
metadata["year"] = metadata['filedAt'].dt.year

# %%
import requests
import datetime

# Filing Render & Download API:
## api key
## https://sec-api.io/login
## https://github.com/janlukasschroeder/sec-api-python/blob/master/README.md#filing-render--download-api
## https://pypi.org/project/sec-api/#filing-render--download-api
extractorApi = ExtractorApi("7caf4a7647d0fe6dd25bad53faa1028f2c47a4633a31b6d6d7c7f19a2ae8a575")


## this part below is to render extracted html to pdf file
for i in range(0, len(metadata)):
    # get the original HTML of section 1 "Business"
    section1_html = extractorApi.get_section(metadata["filingUrl"].iloc[i], "1", "html")
    # get the original HTML of section 1A "Risk Factors"
    section1a_html = extractorApi.get_section(metadata["filingUrl"].iloc[i], "1A", "html")
    
    # combine the HTML content into one large HTML string
    combined_html = section1_html + section1a_html

    # set output filename for the PDF
    year = str(metadata["year"].iloc[i])
    cik = str(metadata["cik"].iloc[i])
    ## change file_name below
    file_name = "section1_1a"
    output_pdf = f"{year}-{cik}-{file_name}.pdf"

    # create pdf file with specified name
    pdfkit.from_string(combined_html, output_pdf)
    
    print(f"The PDF file has been created: {output_pdf}")
