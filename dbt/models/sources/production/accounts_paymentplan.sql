WITH source AS (

    SELECT * FROM {{ source('google_cloud_postgresql_public', 'accounts_paymentplan') }}

),

renamed AS (

    SELECT
        id                               AS plan_id,			
        _fivetran_deleted,			
        _fivetran_synced,			
        ai_blocks,			
        cancel_url,			
        created_at                       AS date_plan_created,			
        currency,			
        data_per_ai_block,			
        flow_refresh_interval,			
        flow_steps,		
        owner_id                         AS old_user_id,			
        paddle_user_id,			
        period_start                     AS date_period_start,			
        predictions,			
        recurring_price,			
        status                           AS plan_status,			
        subscription_id,			
        training_run_price,			
        training_runs,			
        type                             AS plan_type,			
        update_url,			
        updated_at                       AS date_plan_updated,			
        frontegg_tenant_id               AS account_id,			
        frontegg_user_id                 AS user_id		  
    FROM source

)

SELECT *
FROM renamed
