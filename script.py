import pymssql
import psycopg2
import pandas as pd
import glob
import settings as st
from PIL import Image
from resizeimage import resizeimage
from pathlib import Path
from os import path

# Create database connections
seis_conn = pymssql.connect(server=st.seis_server, user=st.seis_user, password=st.seis_password, port=st.seis_port, database=st.seis_database) 
transcribe_conn = psycopg2.connect(host=st.transcribe_server, user=st.transcribe_user, password=st.transcribe_password, database=st.transcribe_database) 

# Load the queries we want to run
with open('seis.sql', 'r') as seis_sql_file, open('transcribe.sql', 'r') as transcribe_sql_file:
    seis_sql = seis_sql_file.read()    
    transcribe_sql = transcribe_sql_file.read()    

# Load the queries into dataframes, add identifier columns and close the seis connection                 
seis_df = pd.read_sql(seis_sql, seis_conn)
transcribe_df = pd.read_sql(transcribe_sql, transcribe_conn)
seis_conn.close()
transcribe_conn.close()

# Transcribe expedition names map to families, but we need to remove the numbers in the expedition names to match it up correctly
transcribe_df['family'] = transcribe_df['expedition'].str.extract('^([A-Z][a-z]+)')

# Join the two tables on institute, family and image name, and then get a list of all seis images which are not on transcribe
merged_df = pd.merge(seis_df, transcribe_df, how='outer', on=['institute', 'family', 'img'], suffixes=('_seis', '_transcribe'))
not_on_transcribe = merged_df.loc[merged_df['expedition'].isnull()]
not_on_transcribe['id'] = not_on_transcribe['id'].astype(int)  # Odd formatting issue
print('Not on transcribe: {}'.format(len(not_on_transcribe)))

# Find the seis file path - we are assuming there is only 1 path we can find
not_on_transcribe['seis_file_path'] = not_on_transcribe['id'].apply(lambda x: glob.glob('{}/**/orig_{}.ims'.format(st.seis_img_dir, x), recursive=True)[0])

# Construct the file paths and copy the files over
def copy_files_to_transcribe(row):
    institute = row['institute'].replace(' ', '_')
    family = row['family'].replace(' ', '_')
    transcribe_file_path = path.join(st.transcribe_img_dir, institute, family, row['img'])
    transcribe_web_path = '/'.join(st.seis_image_server_dir, institute, family, row['img'])
    
    if Path(row['seis_file_path']).is_file():  # If the SEIS file doesn't exist for some odd reason
        return False

    if not Path(transcribe_file_path).is_file():  # If the transcribe file already exists don't copy it
        return transcribe_web_path
    
    with Image.open(row['seis_file_path']) as image:  # Save the image
        new_image = resizeimage.resize_contain(image, [st.max_width, st.max_height])
        img.save(transcribe_file_path, image.format)
        return transcribe_web_path
not_on_transcribe['web_path'] = not_on_transcribe.apply(copy_files_to_transcribe)

# Discard all transcribe tasks which have already been inserted into the database - no idea why I have to do this but shaun does it
exists_sql = 'select exists(select 1 from multimedia where file_path = ?)'
not_on_transcribe['on_transcribe'] = not_on_transcribe['web_path'].apply(lambda x: pd.read_sql(exists_sql, x, transcribe_conn))
insert = not_on_transcribe.loc[not_on_transcribe['on_transcribe'] == False]
if insert.empty:
    exit()

# Get institute details in a separate dataframe and join it into the insert dataframe
institute_names = tuple(insert['institute'].unique())
institute_ids_sql = "select id as 'institute_id', name as 'institute' from institution where name in ({1})".format('?', ','.join('?' + len(institute_names)))  # Construct SQL
institutes = pd.read_sql(institute_ids_sql, transcribe_conn, institute_names)
insert = pd.merge(insert, institutes, how='left', on='institute')  # Join it onto the insert dataframe to get institution ids

# Get project/expedition with less than st.transcribe_expedition_size 
# If there isn't a suitable one then create one
create_project_sql = """insert into project (id, version, created, featured_label, inactive, institution_id, map_init_latitude, map_init_longitude, 
                                            map_init_zoom_level, name, project_type_id, show_map) 
                        values ((select max(id)+1 from project), 0, current_date, ?, TRUE, ?, '-57.4023613632533', '176.396484625', 1, ?, 7134, FALSE)"""
# values: expedition name, institution id, expedition name


# Seperate the dbf into blocks one block of len(for_transcribe_insertion) and blocks of st.transcribe_expedition_size
# Or insert one by one and check for a project/expedition each time (this will be slower but the code will be simpler)

insert_task_sql = 'insert into task (id, created, external_identifier, project_id) values ((select max(id)+1 from task), current_timestamp, ?, ?)'
insert_multimedia_sql = """insert into multimedia (id, created, file_path, file_path_to_thumbnail, mime_type, task_id) 
                           values ((select max(id)+1 from multimedia), current_date, ?, ?, 'image/jpeg', ?)"""

# Haven't included this in the above function as I have a hunch it will be much faster this way
# def insert_transcribe_database_records(row):
#    df.to_sql('db_table2', engine, if_exists='append')


    
# Close database connections
transcribe_conn.close()