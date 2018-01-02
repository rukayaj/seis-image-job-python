import pymssql
import psycopg2
import pandas as pd
import glob
import settings as st
from PIL import Image
from resizeimage import resizeimage
import os

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

# Transcribe expedition names map to families, but we need to remove the numbers in the expedition names to match it up correctly
transcribe_df['family'] = transcribe_df['expedition'].str.extract('^([A-Z][a-z]+)', expand=False)

# Join the two tables on institute, family and image name, and then get a list of all seis images which are not on transcribe
merged_df = pd.merge(seis_df, transcribe_df, how='outer', on=['institute', 'family', 'img'], suffixes=('_seis', '_transcribe'))
not_on_transcribe = merged_df.loc[merged_df['expedition'].isnull() & merged_df['id'].notnull()].copy()
not_on_transcribe['id'] = not_on_transcribe.loc[:, 'id'].astype(int)  # Odd formatting issue
print('Not on transcribe: {}'.format(len(not_on_transcribe)))

# Find the file path for the corresponding seis image using glob - we are assuming there is only 1 path we can find
not_on_transcribe['seis_file_path'] = not_on_transcribe['id'].apply(lambda x: glob.glob('{}/**/orig_{}.ims'.format(st.seis_img_dir, x), recursive=True)).str[0]
df = not_on_transcribe.loc[not_on_transcribe['seis_file_path'].notnull()]  # Drop all the records where we can't find an image to copy

# Get institute details in a separate dataframe and join it into the insert dataframe
institutes_string = ', '.join("'" + elem + "'" for elem in df['institute'].unique())
institutes = pd.read_sql('select id as "institute_id", name as "institute" from institution where name in (' + institutes_string + ')', transcribe_conn)
df = pd.merge(df, institutes, how='left', on='institute')  # Join it onto the insert dataframe to get institution ids

# Construct the file paths and copy the files over
def copy_files_to_transcribe(row):
    institute = row['institute'].replace(' ', '_')
    family = row['family'].replace(' ', '_')
    jpeg_file_name = os.path.splitext(row['img'])[0] + '.jpg'  # add jpg file extension so PIL knows to save it as jpg   
    transcribe_web_path = '/'.join([st.seis_image_server_dir, institute, family, jpeg_file_name])
    
    # Construct the physical file path and create the directory if required
    transcribe_path = os.path.join(st.transcribe_img_dir, institute, family)
    if not os.path.exists(transcribe_path):
        os.makedirs(transcribe_path)
    transcribe_file_path = os.path.join(transcribe_path, jpeg_file_name)
    
    try:
        if not os.path.isfile(row['seis_file_path']):  # If the SEIS file doesn't exist for some odd reason
            return False

        if os.path.isfile(transcribe_file_path):  # If the transcribe file already exists don't copy it
            return transcribe_web_path
        
        with Image.open(row['seis_file_path']) as image:  # Resize and save a new image
            new_image = resizeimage.resize_thumbnail(image, [st.max_width, st.max_height])
            new_image.save(transcribe_file_path)
            return transcribe_web_path
    except: 
        import pdb; pdb.set_trace()
df['web_path'] = df.apply(copy_files_to_transcribe, axis=1)

# Discard all transcribe tasks which have already been inserted into the database - no idea why this might occur but shaun does it
cur = transcribe_conn.cursor()
exists_sql = "select exists(select 1 from multimedia where file_path = %s)"  # Perhaps faster to retrieve entire multimedia table and do a join in pandas?
def check_exists(web_path):
    cur.execute(exists_sql, [web_path])
    return cur.fetchone()[0]
df['on_transcribe'] = df['web_path'].apply(check_exists)
insert = df.loc[df['on_transcribe'] == False]
import pdb; pdb.set_trace()
if insert.empty:  # If there's nothing to insert stop the script
    exit()

# Get project/expedition with less than st.transcribe_expedition_size 
select_project_sql = """select """
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