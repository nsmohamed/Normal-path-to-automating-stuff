import pandas as pd
import unicodedata
import concurrent.futures
import os
import re
import sys

if len(sys.argv) != 2:
     print("wrong file number")
     sys.exit(1)

source_file_n = sys.argv[1] # importing new file by the file number

# Path to the directory with your files
path_main_file = 'main file'
products_file_path = 'Products file'

def read_excel_file(file_path, columns=None):
    if columns:
        return pd.read_excel(file_path, usecols=columns)
    else:
        return pd.read_excel(file_path)

Path_new_file = f'new_file_path ({source_file_n}).xlsx'

with concurrent.futures.ThreadPoolExecutor() as executor:
    SPST_new_file,products_file_main = executor.map(read_excel_file,[Path_new_file,products_file_path])


SPST_main = pd.read_csv(path_main_file)
SPST_main['Date'] = pd.to_datetime(SPST_main['Date'])

SPST = SPST_new_file
SPST['source_file'] = os.path.basename(Path_new_file)
SPST['source_file_n'] = SPST['source_file'].apply(lambda x: re.search(r'\((\d+)\)', x).group(1) if re.search(r'\((\d+)\)', x) else None)
SPST['source_file_number'] = SPST['source_file_n'].astype(int)
SPST['Date'] = pd.to_datetime(SPST['Date'])

products_file = products_file_main

def Sponsored_Product_SPST(SPST,SPST_main):
    
    # formula to get the type of targeting
    def determine_targeting(SPST):
        if SPST['Match Type'] == '-' and 'category' in SPST['Targeting']:
            return 'Category Targeting'
            
        elif SPST['Match Type'] == '-' and 'asin' in SPST['Targeting']:
            return 'ASIN Targeting'
        
        elif SPST['Match Type'] != '-':
            return 'Keyword Targeting'

        else:
            return 'Automatic Targeting'

    # formula to get the specific (Keyword - Category - ASIN) we're targeting
    def Keyword_category_asin(SPST):
            try:
                if SPST['Match Type'] == '-':
                    start_pos = SPST['Targeting'].find('"')+1
                    end_pos = SPST['Targeting'].rfind('"')-1
                    if start_pos > 0 and end_pos > start_pos:
                        # Extract substring from one position after '=' to the end
                        return SPST['Targeting'][start_pos:end_pos]
                    else:
                        # if '=' is not found, return the full 'Targeting' string
                        return SPST['Targeting']
                else:
                    # if '-' isn't found, return full 'Targeting' string
                    return SPST['Targeting']
            
            except:
                # if there is an error return the 'Targeting' string
                return 'Automatic Targeting'


    SPST['Targeting Type'] = SPST.apply(determine_targeting, axis=1)
    SPST['Keyword_category_asin'] = SPST.apply(Keyword_category_asin, axis=1)


    # removing unicodedata and converting to float
    def clean_convert_to_numeric(column):
        def remove_rtl_characters(text):
            return ''.join(char for char in text if unicodedata.category(char) != 'Lm' and unicodedata.category(char) != 'Cf')
        
        cleaned_column = column.astype(str).apply(remove_rtl_characters)
        cleaned_column = cleaned_column.str.replace('ج.م.', '', regex=False)
        return pd.to_numeric(cleaned_column, errors='coerce')

    SPST['14 Day Total Sales'] = clean_convert_to_numeric(SPST['14 Day Total Sales'])
    SPST['Spend'] = clean_convert_to_numeric(SPST['Spend']) 
    products_file['ASIN'] = products_file['ASIN'].str.lower()

    SPST = SPST.merge(products_file,how='left',left_on='Customer Search Term', right_on='ASIN')
    SPST['Brand Description'].fillna('', inplace=True)
    # remove rows with 0 impressions
    cleaned_file = SPST[SPST['Impressions'] != 0]

    # Renaming columns
    cleaned_file = cleaned_file.rename(columns={'14 Day Total Sales': 'Total Sales', '14 Day Total Orders (#)': 'Total Orders', '14 Day Total Units (#)': 'Total Units','14 Day Advertised ASIN Units (#)':'Total Units Advertised ASINS','14 Day Brand Halo ASIN Units (#)':'Total Units Brand Halo ASIN', '14 Day Advertised ASIN Sales':'Total Sales Advertised ASINS','14 Day Brand Halo ASIN Sales':'Total Sales Brand Halo ASIN'})
    
    # labeling branded and non branded based on keyword - search term - asin
    def Branded_NotBranded(row):
        keyword = str(row['Keyword_category_asin']).lower()  # Convert to string and lower case
        search_term = str(row['Customer Search Term']).lower()  # Convert to string and lower case
        brand_description = str(row['Brand Description']).lower()
        
        # List of brands in lowercase
        brands = ['juhayna', 'molto', 'rameda', 'masrawy', 'biscomisr', 'seoudi', 'zahran', 'baraka', 'banque misr', 'cleopatra', 'edfa3ly', 'el araby', 'fresh', 'goldi', 'mezza luna', "temmy's"]

        # checking if our brand is mentioned either in search term or in targeting, this helps with deeper investigation to check our performance branded-nonbranded
        
        if any(brand in keyword for brand in brands):
                return 'Branded'
        
        if any(brand in search_term for brand in brands):
                return "Branded search term"

        # check if ASIN product isn't null, meaning that the customer landed on our product
        if  any(brand in brand_description for brand in brands):
                return 'Branded searched ASIN'

        return 'Not Branded'


    
    cleaned_file['Branded_NotBranded'] = cleaned_file.apply(Branded_NotBranded, axis=1)


    # sort by date
    cleaned_file.sort_values(by=['Date'], inplace=True)

    #fillna
    cleaned_file.fillna('', inplace=True)

    # getting brand name
    cleaned_file['Brand'] = cleaned_file['Campaign Name'].str.split('_').str[0]

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
    'Ad Group Name',
    'Brand',
    'Targeting Type',
    'Keyword_category_asin',
    'Branded_NotBranded',
    'Match Type',
    'Customer Search Term',
    'Impressions',
    'Clicks',
    'Spend',
    'Total Sales',
    'Total Orders',
    'Total Units',
    'Total Units Advertised ASINS',
    'Total Sales Advertised ASINS',    
    'Total Units Brand Halo ASIN',
    'Total Sales Brand Halo ASIN',
    'source_file_number'])

    combined_df = pd.concat([cleaned_file,SPST_main])
    combined_df.sort_values(by=['source_file_number','Date','Campaign Name','Ad Group Name','Keyword_category_asin','Match Type','Customer Search Term'], ascending=[False,True,True,True,True,True,True], inplace=True)
    #remove duplicated
    
    combined_df.drop_duplicates(subset=['Date','Campaign Name','Ad Group Name','Keyword_category_asin','Match Type','Customer Search Term'], keep='first', inplace=True)


    return combined_df.to_csv(r"updated_file_path.csv", index=False)


# #final_cleaned_table(SPST)


Sponsored_Product_SPST(SPST,SPST_main)