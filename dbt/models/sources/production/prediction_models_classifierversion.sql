WITH source AS (

    SELECT * FROM {{ source('public', 'prediction_models_classifierversion') }}

), renamed AS (

    SELECT
        id                                            AS version_id,			
        classifier_id,		
        CAST(created_at  AS TIMESTAMP)                AS date_version_created,		
        --default,	
        evaluation,		
        fake,		
        CAST(owner_id AS STRING)                      AS old_user_id,
        frontegg_user_id                              AS user_id,
        frontegg_tenant_id                            AS workspace_id,		
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
        valohai_version_name 
    FROM 
        source

)

SELECT *
FROM renamed