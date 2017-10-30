import pandas as pd
import  numpy as np
import datetime as dt

# Reading input files and declaring output files
input_filename = ".\\input\\itcont.txt"
output1_filename = ".\\output\\medianvals_by_zip.txt"
output2_filename=".\\output\\medianvals_by_date.txt"

# Reading a major dataframe with required columns

df = pd.read_csv(input_filename, sep ="|", index_col= False, usecols = [0, 10, 13, 14, 15], names = ['CMTE_ID', 'ZIP_CODE', 'TRANSACTION_DT', 'TRANSACTION_AMT', 'OTHER_ID'],
                 dtype = {'CMTE_ID': str, 'ZIP_CODE' : str , 'TRANSACTION_DT' : str, 'TRANSACTION_AMT' : int , 'OTHER_ID' : str})

# Creating two data frames - for contributions per zip_code and contributions per date
df1 = df[ ['CMTE_ID', 'ZIP_CODE', 'TRANSACTION_AMT', 'OTHER_ID']]
df2 = df[ ['CMTE_ID', 'TRANSACTION_DT', 'TRANSACTION_AMT', 'OTHER_ID']]

# Filtering out rows with OTHER_ID not empty, CMTE_ID empty and TRANSACTION_AMT empty
df1 = df1[(df1['OTHER_ID'].isnull()) & (df1['CMTE_ID'].notnull()) & (df1['TRANSACTION_AMT'].notnull())]
df2 = df2[(df2['OTHER_ID'].isnull()) & (df1['CMTE_ID'].notnull()) & (df1['TRANSACTION_AMT'].notnull())]

# Checking for valid ZIP_CODE - eliminated rows with no zip_code or malformed zip_code
df1 = df1[(df1['ZIP_CODE'].notnull())]
df1 = df1[df1['ZIP_CODE'].apply(lambda x : x.isdigit() and len(x) >= 5)]
df1['ZIP_CODE'] = df1['ZIP_CODE'].astype(str).str[:5]
df1['ID'] = 1

# Checking for valid TRANSACTION_DT - eliminated rows with no date or malformed date
df2 = df2[df2['TRANSACTION_DT'].notnull()]
df2['TRANSACTION_DT'] = df2['TRANSACTION_DT'].astype(int).astype(str)
df2['TRANSACTION_DT'] =  pd.to_datetime(df2['TRANSACTION_DT'], format="%m%d%Y")
df2['TRANSACTION_DT'] = df2['TRANSACTION_DT'].astype(str)
df2['ID'] = 1

# Rolling sum of contribution amounts as per zip_code
df1['ROLLING_SUM'] = df1.groupby(['ZIP_CODE', 'CMTE_ID'])['TRANSACTION_AMT'].cumsum()
df1['ID'] = df1.groupby(['ZIP_CODE', 'CMTE_ID'])['ID'].cumsum()
df1['ID'] = df1['ID'].astype(int)

# Rolling sum of contribution amounts as per transaction date
df2['ROLLING_SUM'] = df2.groupby(['TRANSACTION_DT', 'CMTE_ID'])['TRANSACTION_AMT'].cumsum()
df2['ID'] = df2.groupby(['TRANSACTION_DT', 'CMTE_ID'])['ID'].cumsum()
df2['ID'] = df2['ID'].astype(int)

# Creating a unique ID for identifying unique contributions per zip_code per committee
df1['UNIQUE_ID'] = df1['ZIP_CODE'].astype(str) + df1['CMTE_ID'].astype(str)

# Creating a unique ID for identifying unique contributions per date per committee
df2['UNIQUE_ID'] = df2['TRANSACTION_DT'].astype(str) + df2['CMTE_ID'].astype(str)

# Grouping the Unique ID to find rolling median of TRANSACTION_AMT for each unique ID
df11 =  (df1.groupby('UNIQUE_ID')['TRANSACTION_AMT'].expanding().median().reset_index('UNIQUE_ID'))
df22 =  (df2.groupby('UNIQUE_ID')['TRANSACTION_AMT'].expanding().median().reset_index('UNIQUE_ID'))


# Removing and stacking all valid values of rolling median as per zip_codes
#df1['ROLLING_MEDIAN']  = df11.stack().groupby(level=0).first().reindex(df11.index).round()
#df1['ROLLING_MEDIAN'] = df1['ROLLING_MEDIAN'].astype(int)
df1['ROLLING_MEDIAN'] = df11['TRANSACTION_AMT'].sort_index().round(decimals = 0).astype(int)

# Removing and stacking all valid values of rolling median as per transaction dates
#df2['ROLLING_MEDIAN']  = df22.stack().groupby(level=0).first().reindex(df22.index).round()
#df2['ROLLING_MEDIAN'] = df2['ROLLING_MEDIAN'].astype(int)
df2['ROLLING_MEDIAN'] = df22['TRANSACTION_AMT'].sort_index().round(decimals = 0).astype(int)

# Extract the required output columns
df_new_zip = pd.DataFrame(df1[['CMTE_ID', 'ZIP_CODE', 'ROLLING_MEDIAN', 'ID', 'ROLLING_SUM']])


# Eliminating duplicate CMTE ids with intermediate rolling sum values and extracting rows with total contribution values
df2['TRANSACTION_DT'] = df2['TRANSACTION_DT'].apply(lambda x: x.replace('-',''))
df_new_dt = pd.DataFrame(df2[['CMTE_ID', 'TRANSACTION_DT', 'ROLLING_MEDIAN', 'ID', 'ROLLING_SUM']]).drop_duplicates(subset = 'CMTE_ID',keep='last').sort_values('ID',ascending = False)

# Writing the output to respective output files
df_new_zip.to_csv(output1_filename, sep='|', index=False,header =False)
df_new_dt.to_csv(output2_filename, sep='|', index=False,header =False)

