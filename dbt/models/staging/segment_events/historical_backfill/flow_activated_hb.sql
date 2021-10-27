WITH workflows_workflow AS (

    SELECT 
        flow_id,
        flow_status,
        user_id
    FROM 
        {{ref('workflows_workflow')}}

), onboarded_users AS (

    SELECT
        user_id,
        user_email_address
    FROM
        {{ref('onboarded_users')}}

), flow_active AS (
    
    SELECT
        name,
        flow_id,
        flow_status,
        user_id,
        user_email_address 
    FROM workflows_workflow wfw 
    INNER JOIN onboarded_users obu ON wfw.user_id = obu.user_id
    WHERE flow_status = 'active'

)

SELECT 
    *
FROM flow_active