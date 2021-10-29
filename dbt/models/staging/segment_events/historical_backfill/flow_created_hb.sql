WITH workflows_workflow AS (

    SELECT 
        flow_id,
        date_flow_created,
        user_id,
        account_id
    FROM 
        {{ref('workflows_workflow')}}

)


SELECT 
    *
FROM workflows_workflow
