WITH source AS (

    SELECT * FROM {{ source('public', 'prediction_models_classifier') }}

), renamed AS (

    SELECT
        id	                                     AS	classifier_id,	
        created_at,		               
        dataset_id                               AS aiblock_id,	
        emoji,		
        hitl_setting,		
        is_enabled,	
        max_error_rate,	
        CAST(owner_id AS STRING)                 AS old_user_id,
        frontegg_user_id                         AS user_id,
        frontegg_tenant_id                       AS workspace_id,		
        project_id,	
        status,		
        updated_at,	
        valohai_deployment_id 
    FROM 
        source

)

SELECT *
FROM renamed