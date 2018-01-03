import pymssql
import psycopg2
import pandas as pd
import glob
import settings as st
from PIL import Image
from resizeimage import resizeimage
import os

# Create database connections and read in the initial data
seis_conn = pymssql.connect(server=st.seis_server, user=st.seis_user, password=st.seis_password, port=st.seis_port, database=st.seis_database) 
transcribe_conn = psycopg2.connect(host=st.transcribe_server, user=st.transcribe_user, password=st.transcribe_password, database=st.transcribe_database, port=st.transcribe_port) 
transcribe_sql = """select task.external_identifier as img, project.name as expedition, institution.name as institute
                    from task left join project on project.id = task.project_id left join institution on institution.id = project.institution_id"""
seis_sql = """select ims_document.ims_id as 'id', ims_document.ims_name as 'img', childFolder.ims_name as 'family',
                ims_document.ims_upload_date as 'upload_date', parentFolder.ims_name as 'institute'
            from ims_document
                inner join ims_folder folder on ims_document.ims_folder = folder.ims_id
                inner join ims_folder_add_lang childFolder on folder.ims_id = childFolder.ims_folder
                inner join ims_folder_add_lang parentFolder on folder.ims_parent_folder = parentFolder.ims_folder
            where (select ims_folder.ims_parent_folder from ims_folder where ims_folder.ims_id = parentFolder.ims_folder) = 951"""            
seis_df = pd.read_sql(seis_sql, seis_conn)
transcribe_df = pd.read_sql(transcribe_sql, transcribe_conn)
seis_conn.close()  # We don't need this one any more, but the transcribe one is used throughout the script

# Transcribe expedition names map to families, but we need to remove the numbers in the expedition names to match it up correctly
transcribe_df['family'] = transcribe_df['expedition'].str.extract('^([A-Z][a-z]+)', expand=False)

# Join the two tables on institute, family and image name, and then get a list of all seis images which are not on transcribe
merged_df = pd.merge(seis_df, transcribe_df, how='outer', on=['institute', 'family', 'img'])
not_on_transcribe = merged_df.loc[merged_df['expedition'].isnull() & merged_df['id'].notnull()].copy()
not_on_transcribe['id'] = not_on_transcribe.loc[:, 'id'].astype(int)  # Odd formatting issue
print('Not on transcribe: {}'.format(len(not_on_transcribe))) # not_on_transcribe.to_csv('not_on_transcribe.csv')
exit()

# Find the file path for the corresponding seis image using glob - we are assuming there is only 1 path we can find
not_on_transcribe['seis_file_path'] = not_on_transcribe['id'].apply(lambda x: glob.glob('{}/**/orig_{}.ims'.format(st.seis_img_dir, x), recursive=True)).str[0]
df = not_on_transcribe.loc[not_on_transcribe['seis_file_path'].notnull()]  # Drop all the records where we can't find an image to copy
if df.empty:
    exit()

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
    
    if not os.path.isfile(row['seis_file_path']):  # If the SEIS file doesn't exist for some odd reason
        return False
    if os.path.isfile(transcribe_file_path):  # If the transcribe file already exists don't copy it
        return transcribe_web_path
        
    with Image.open(row['seis_file_path']) as image:  # Resize and save a new image
        new_image = resizeimage.resize_thumbnail(image, [st.max_width, st.max_height])
        new_image.save(transcribe_file_path)
        return transcribe_web_path
df['web_path'] = df.apply(copy_files_to_transcribe, axis=1)

# Check to see if the tasks have already been inserted into the multimedia table in the transcribe db - Shaun did this for some reason
tr_multimedia = pd.read_sql('select file_path as task_on_transcribe from multimedia', transcribe_conn)
df = pd.merge(df, tr_multimedia, how='left', left_on='web_path', right_on='task_on_transcribe') 
for_insert = df.loc[df['task_on_transcribe'].isnull()]
if for_insert.empty:  # If there's nothing to insert stop the script
    import pdb; pdb.set_trace()
    exit()

# Two functions below to insert a transcribe project and task + multimedia (which calls get_or_create_project first)
cur = transcribe_conn.cursor()
def get_or_create_project(institution_id, family):
    select_project_sql = """SELECT project.id, featured_label, count(task.project_id) AS task_count 
                            FROM project LEFT JOIN task ON task.project_id = project.id 
                            WHERE featured_label LIKE %s GROUP BY project.id ORDER BY length(featured_label) DESC, featured_label DESC"""
    projects = pd.read_sql(select_project_sql, transcribe_conn, params=[family + '%'])
        
    if not projects.empty:
        if projects.iloc[0]['task_count'] < st.transcribe_expedition_size:  # If there's a suitable expedition not full of tasks, return that project id 
            return projects.iloc[0]['id']
    
    new_expedition_name = family + ' ' + str(len(projects) + 1)  # Otherwise make a new expedition/project and return the id
    insert_project_sql = """INSERT INTO project(featured_label, institution_id, name, inactive, project_type_id, show_map, created, id)
                            VALUES (%s, %s, %s, TRUE, 7134, FALSE, CURRENT_DATE, (SELECT MAX(id)+1 FROM project)) RETURNING id"""
    cur.execute(insert_project_sql, [new_expedition_name, institution_id, new_expedition_name])
    return cur.fetchone()[0]  # id of newly inserted row
    
def insert_transcription_task(row):
    project_id = get_or_create_project(row['institute_id'], row['family'])
    
    # Insert the task
    insert_task_sql = 'INSERT INTO task (id, created, external_identifier, project_id) VALUES ((SELECT MAX(id)+1 FROM task), current_timestamp, %s, %s) RETURNING id'
    cur.execute(insert_task_sql, [row['img'], int(project_id)])
    task_id = cur.fetchone()[0] 
    
    # Insert the multimedia
    insert_multimedia_sql = """INSERT INTO multimedia (id, created, mime_type, file_path, file_path_to_thumbnail, task_id) 
                               VALUES ((SELECT MAX(id)+1 FROM multimedia), current_date, 'image/jpeg', %s, %s, %s) RETURNING id"""
    cur.execute(insert_multimedia_sql, [row['web_path'], row['web_path'], int(task_id)])
    transcribe_conn.commit()
    
    return task_id
for_insert['new_task_id'] = for_insert.apply(insert_transcription_task, axis=1)  # This might be a faster alternative: df.to_sql('db_tablename', engine, if_exists='append')
   
# Close database connections
transcribe_conn.commit()
transcribe_conn.close()

import pdb; pdb.set_trace()