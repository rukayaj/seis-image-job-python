import pymssql
import psycopg2
import pandas as pd
from settings import *


# Load the queries we want to run
with open('seis.sql', 'r') as seis_sql, open('transcribe.sql', 'r') as transcribe_sql:
    seis_sql = sql_file.read()    
    transcribe_sql = sql_file.read()
    
    # Create database connections
    seis_conn = pymssql.connect(server=seis_server, user=seis_user, password=seis_password, port=seis_port, database=seis_database) 
    transcribe_conn = psycopg2.connect(host=transcribe_server, user=transcribe_user, password=transcribe_password, database=transcribe_database) 

    # Load the queries into dataframes                    
    seis_df = pd.read_sql(seis_query, seis_conn)
    transcribe_df = pd.read_sql(transcribe_query, transcribe_conn)

    # Transcribe expedition names map to families, but we need to remove the numbers in the expedition names to match it up correctly
    transcribe_df['family'] = transcribe_df['expedition'].str.extract('^([A-Z][a-z]+)')

    # Add identifier column
    transcribe_df['on_transcribe'] = True
    seis_df['on_seis'] = True

    # Join the two tables on institute, family and image name, and then get a list of all seis images which are not on transcribe
    merged_df = pd.merge(seis_df, transcribe_df, how='outer', on=['institute', 'family', 'img'], suffixes=('_seis', '_transcribe'))
    not_on_transcribe = merged_df[merged_df['on_transcribe'].isnull()]

seis_conn.close()
transcribe_conn.close()
    
def error_checking(transcribe_df, seis_df):
    transcribe_df['img_name'] = transcribe_df['img'].str.extract('^([^\.]+)')
    seis_df['img_name'] = seis_df['img'].str.extract('^([^\.]+)')

    # Double check that we haven't accidentally removed more characters than we meant to 
    temp = seis_df['img'].str.len() - seis_df['img_name'].str.len()
    print('SEIS - {} specimens with a weird long file extension, {} specimens with no file extension'.format(len(temp[temp > 4]), len(temp[temp < 4])))
    temp = transcribe_df['img'].str.len() - transcribe_df['img_name'].str.len()
    print('Transcribe - {} specimens with a weird long file extension, {} specimens with no file extension'.format(len(temp[temp > 4]), len(temp[temp < 4])))
    
    # This is misleading because there are some images on transcribe which are not on seis as well as vice versa
    print('{} more specimens in SEIS than in transcribe'.format(len(seis_df) - len(transcribe_df)))

    # not_on_seis = merged_df[merged_df['on_seis'].isnull()]
    # 416 on transcribe which are not on seis... wtf. 
    # Is there something odd about full join? 
    # temp = pd.merge(transcribe_df, seis_df, how='left', on=['institute', 'family', 'img'], suffixes=('_transcribe', '_seis'))
    # temp_not_on_seis = temp[temp['on_seis'].isnull()]
    # len(temp_not_on_seis)

    # Ok there are a whole bunch of duplicates
    seis_duplicates = seis_df.loc[seis_df.duplicated(['institute', 'family', 'img'])]
    transcribe_duplicates = transcribe_df.loc[transcribe_df.duplicated(['institute', 'family', 'img'])]

    # Write to csv
    not_on_seis.to_csv('not_on_seis.csv')
    not_on_transcribe.to_csv('not_on_transcribe.csv')
    seis_duplicates.to_csv('seis_duplicates.csv')
    transcribe_duplicates.to_csv('transcribe_duplicates.csv')

import pdb; pdb.set_trace()

# Copy 
import glob
for filename in glob.iglob('src/**/*.c', recursive=True):
    print(filename)