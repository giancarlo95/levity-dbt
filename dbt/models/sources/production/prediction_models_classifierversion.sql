WITH source AS (

    SELECT * FROM {{ source('google_cloud_postgresql_public', 'prediction_models_classifierversion') }}

), renamed AS (

    SELECT
        id                                            AS version_id,			
        classifier_id,		
        CAST(created_at  AS TIMESTAMP)                AS date_version_created,		
        --default,	
        evaluation,		
        fake,		
        CAST(owner_id AS STRING)             AS old_user_id,
        frontegg_user_id                     AS user_id,
        frontegg_tenant_id                   AS account_id,		
        performance_score,		
        seconds_left,		
        status,	
        template,		
        training_progress,		
        CAST(updated_at  AS TIMESTAMP)                AS date_version_updated,		
        valohai_endpoint_id,	
        valohai_endpoint_url,	
        valohai_execution_id,	
        valohai_version_id,	
        valohai_version_name,
        --_airbyte_emitted_at,	
        --_airbyte_production_accounts_paymentplan_hashid
        _fivetran_deleted,
        _fivetran_synced 
    FROM 
        source

)

SELECT *
FROM renamed