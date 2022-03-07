WITH source AS (

    SELECT * FROM {{ source('paddle_Paddle_paddle', 'users')}}

), renamed AS (

    SELECT
        cancel_url,			
        last_payment_amount,			
        last_payment_currency,			
        last_payment_date,			
        marketing_consent,		
        next_payment_amount,			
        next_payment_currency,			
        next_payment_date,			
        paused_at,			
        paused_from,			
        paused_reason,			
        payment_information_card_type,			
        payment_information_expiry_date,			
        payment_information_last_four_digits,			
        payment_information_payment_method,			
        plan_id,			
        precog_delta_key,			
        signup_date,			
        state,			
        subscription_id,			
        update_url,			
        user_email,			
        user_id,			
        users_precog_key			
    FROM 
        source

)

SELECT *
FROM renamed