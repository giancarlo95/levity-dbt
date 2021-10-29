WITH workflows_workflow AS (

    SELECT
        flow_id,
        flow_status,
        user_id,
        account_id
    FROM 
        {{ref('workflows_workflow')}}

)

SELECT 
    *
FROM workflows_workflow
WHERE flow_status = 'active'