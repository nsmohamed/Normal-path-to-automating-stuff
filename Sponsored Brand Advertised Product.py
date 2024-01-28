import pandas as pd
import unicodedata
import re
import sys
import os
import concurrent.futures

source_file_n = sys.argv[1] # importing new file by using the file number

# Path to the directory with your files
products_file_path = 'product_file'

def read_csv_file(file_path, columns=None):
    if columns:
        return pd.read_csv(file_path, usecols=columns)
    else:
        return pd.read_csv(file_path)

path_main_file = f'new_file_path ({source_file_n}).csv'

with concurrent.futures.ThreadPoolExecutor() as executor:
    SBASIN_main,products_file_main = executor.map(read_csv_file,[path_main_file,products_file_path])#[None,columns_of_interest,products_file_columns]


SBASIN_main = pd.read_csv(path_main_file)
SBASIN_main['Date'] = pd.to_datetime(SBASIN_main['Date'])

SBASIN = SBASIN_main
SBASIN['source_file'] = os.path.basename(path_main_file)
SBASIN['source_file_n'] = SBASIN['source_file'].apply(lambda x: re.search(r'\((\d+)\)', x).group(1) if re.search(r'\((\d+)\)', x) else None)
SBASIN['source_file_number'] = SBASIN['source_file_n'].astype(int)
SBASIN['Date'] = pd.to_datetime(SBASIN['Date'])

products_file = products_file_main


def Sponsored_Brand_ASIN(SBASIN):

    # removing unicodedata and converting to float for search impression share
    def clean_convert_to_numeric(column):
        def remove_rtl_characters(text):
            return ''.join(char for char in text if unicodedata.category(char) != 'Lm' and unicodedata.category(char) != 'Cf')
        
        cleaned_column = column.astype(str).apply(remove_rtl_characters)
        cleaned_column = cleaned_column.str.replace('ج.م.', '', regex=False)
        cleaned_column = cleaned_column.astype(float)
        return cleaned_column

    SBASIN['14 Day Total Sales'] = clean_convert_to_numeric(SBASIN['14 Day Total Sales'])
    SBASIN['14 Day New-to-brand Sales'] = clean_convert_to_numeric(SBASIN['14 Day Total Sales'])

    # Sort by 'source file name' descending, then by date, campaign name, adgroup name, asin

    SBASIN.sort_values(by=['Date','Campaign Name','Attribution type','Purchased ASIN'], ascending=[True,True,True,True], inplace=True)

    # Merge to get data about the product name and category
    cleaned_file = SBASIN.merge(products_file, how='left', left_on='Purchased ASIN', right_on= 'ASIN')
    
    # Drop columns after merge
    cleaned_file.drop(columns=['ASIN','Product Name'], inplace=True)

    # Renaming columns
    cleaned_file = cleaned_file.rename(columns={'Brand Description':'Brand','Category Description':'Category','Purchased ASIN':'ASIN','Viewable impressions':'Vimp','14-day Detail Page Views (DPV)':'DPV','14 Day Total Orders (#)':'Total Orders','14 Day Total Units (#)':'Total Units','14 Day Total Sales':'Total Sales','14 Day New-to-brand Orders (#)':'NTB Orders','14 Day New-to-brand Sales':'NTB Sales','14 Day New-to-brand Units (#)':'NTB Units','14-Day Total Orders (#) \u2013 (Click)':'Total Orders(click)','14-Day Total Units (#) \u2013 (Click)':'Total Units(click)','14-Day Total Sales \u2013 (Click)':'Total Sales(click)','14-Day New-to-brand Orders (#) \u2013 (Click)':'NTB Orders(click)','14-Day New-to-brand Sales \u2013 (Click)':'NTB Sales (click)','14-Day New-to-Brand Units (#) \u2013 (Click)':'NTB Units (click)'})

    # sort by date
    cleaned_file.sort_values(by=['Date'], ascending=False, inplace=True)

    # fillna
    cleaned_file.fillna("Not Advertised ASIN", inplace=True)

    #day of week
    cleaned_file['Day of week'] = cleaned_file['Date'].dt.dayofweek
    # map the day of the week number to its name
    cleaned_file['Day of week'] = cleaned_file['Day of week'].map({
        0: 'Monday',
        1: 'Tuesday',
        2: 'Wednesday',
        3: 'Thursday',
        4: 'Friday',
        5: 'Saturday',
        6: 'Sunday'
    })

    #Re-arranging columns
    cleaned_file = cleaned_file.reindex(columns=['Date',
    'Day of week',
    'Campaign Name',
    'Attribution type',
    'ASIN',
    'Brand',
    'Product',
    'Category',
    'Total Orders',
    'Total Units',
    'Total Sales',
    'NTB Orders',
    'NTB Sales',
    'NTB Units',
    'source_file_number'])


    return cleaned_file

#final_cleaned_table(SBASIN)

Sponsored_Brand_ASIN(SBASIN).to_csv(r"updated_file_path.csv", index=False)
