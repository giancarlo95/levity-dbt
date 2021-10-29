WITH workflows_workflow AS (

    SELECT 
        flow_id,
        date_flow_created,
        user_id,
        account_id
    FROM 
        {{ref('workflows_workflow')}}

), onboarded_users AS (

    SELECT
        user_id,
        user_email_address
    FROM
        {{ref('onboarded_users')}}
        
)


SELECT 
    flow_id,
    date_flow_created,
    wfw.user_id,
    account_id,
    user_email_address
FROM workflows_workflow wfw 
INNER JOIN onboarded_users ob ON wfw.user_id = ob.user_id
WHERE TIMESTAMP_DIFF(TIMESTAMP "2021-09-21 00:00:00+00", date_flow_created, HOUR)>0
