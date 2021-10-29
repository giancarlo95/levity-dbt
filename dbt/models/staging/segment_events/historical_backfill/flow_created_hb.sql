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
WHERE TIMESTAMP_DIFF(TIMESTAMP "2021-09-21 00:00:00+00", date_flow_created, HOUR)>0
