select 
	task.external_identifier as img, 
	project.name as expedition, 
	featured_owner as institute
from task 
	left join project on project.id = task.project_id