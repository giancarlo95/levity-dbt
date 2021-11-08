WITH source AS (

    SELECT * FROM {{ source('google_cloud_postgresql_public', 'prediction_models_classifier') }}

), renamed AS (

    SELECT
        id	                                     AS	classifier_id,	
        created_at,		               
        dataset_id                               AS aiblock_id,	
        emoji,		
        hitl_setting,		
        is_enabled,	
        max_error_rate,	
        CAST(owner_id AS STRING)             AS old_user_id,
        frontegg_user_id                     AS user_id,
        frontegg_tenant_id                   AS account_id,		
        project_id,	
        status,		
        updated_at,	
        valohai_deployment_id,
        --_airbyte_emitted_at,	
        --_airbyte_production_accounts_paymentplan_hashid
        _fivetran_deleted,
        _fivetran_synced 
    FROM 
        source

)

SELECT *
FROM renamed