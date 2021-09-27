{{
  config(
    materialized = 'table',
    )
}}

WITH created_flow AS (

    SELECT * FROM {{ref('created_flow')}}
       
), created_trigger AS (

    SELECT * FROM {{ref('created_trigger')}}

), connected_ai_block AS (

    SELECT * FROM {{ref('connected_ai_block')}}

), created_action AS (

    SELECT * FROM {{ref('created_action')}}

), onboarded_users AS (

    SELECT * FROM {{ref('onboarded_users')}}

), workflows_workflow AS (

    SELECT 
        DISTINCT user_id,
        'flow_activated' AS activation_flag
    FROM {{ref('workflows_workflow')}}
    WHERE 
        flow_status='active'
)

SELECT 
    onboarded_users.user_id,
    onboarded_users.user_email_address,
    onboarded_users.date_user_onboarded,
    created_flow.date_first_flow_created,
    created_trigger.date_first_trigger_created,
    connected_ai_block.date_first_aiblock_connected,
    created_action.date_first_action_created,
    workflows_workflow.activation_flag
FROM onboarded_users
LEFT JOIN created_flow ON 
    onboarded_users.user_id=created_flow.user_id
LEFT JOIN created_trigger ON
    onboarded_users.user_id=created_trigger.user_id
LEFT JOIN connected_ai_block ON
    onboarded_users.user_id=connected_ai_block.user_id
LEFT JOIN created_action ON
    onboarded_users.user_id=created_action.user_id
LEFT JOIN workflows_workflow ON
    onboarded_users.user_id=workflows_workflow.user_id