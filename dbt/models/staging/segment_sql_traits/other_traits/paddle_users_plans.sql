WITH paddle_users AS (

    SELECT 
        *
    FROM 
        {{ref('paddle_users')}}

), paddle_plans AS (

    SELECT 
        *
    FROM    
        {{ref('paddle_plans')}}

)

SELECT				
    paused_at                   AS subscription_paused_at,			
    paused_from                 AS subscription_paused_from,			
    paused_reason               AS subscription_paused_reason,						
    plan_id,					
    signup_date,			
    state                       AS subscription_state,			
    subscription_id,					
    user_email,			
    user_id,
    pp.name                     AS plan_name,
    pp.billing_type             AS plan_billing_type,
    pp.billing_period           AS plan_billing_period,
    pp.trial_days               AS plan_trial_days
FROM paddle_users pu
INNER JOIN paddle_plans pp ON pp.id=pu.plan_id