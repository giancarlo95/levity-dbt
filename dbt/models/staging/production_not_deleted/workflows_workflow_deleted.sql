WITH workflows_workflow AS (

    SELECT
       * 
    FROM 
       {{ref('workflows_workflow')}}

), workflow_deleted AS (

    SELECT 
       *
    FROM 
       {{ref('workflow_deleted')}}

)

SELECT 
    workflows_workflow.flow_id,						
    date_flow_created,			
    flow_description,		
    name,			
    old_user_id,
    user_id,
    account_id,	
    flow_status,		
    date_flow_updated,
    _airbyte_emitted_at,	
    _airbyte_production_workflows_workflow_hashid 
FROM 
    workflows_workflow
LEFT JOIN 
    workflow_deleted ON workflow_deleted.flow_id=workflows_workflow.flow_id
WHERE
    workflow_deleted.flow_id IS NULL