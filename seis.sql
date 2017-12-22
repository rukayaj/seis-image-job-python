select 
	ims_document.ims_id as 'id',
	ims_document.ims_upload_date as 'upload_date', 
	ims_document.ims_name as 'img', 
	childFolder.ims_name as 'family', 
	parentFolder.ims_name as 'institute'
from ims_document
	inner join ims_folder folder on ims_document.ims_folder = folder.ims_id
	inner join ims_folder_add_lang childFolder on folder.ims_id = childFolder.ims_folder
	inner join ims_folder_add_lang parentFolder on folder.ims_parent_folder = parentFolder.ims_folder
where (select ims_folder.ims_parent_folder from ims_folder where ims_folder.ims_id = parentFolder.ims_folder) = 951