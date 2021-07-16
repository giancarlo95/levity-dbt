WITH source AS (

    SELECT * FROM {{ source('public', 'production_prediction_models_classifier') }}

), renamed AS (

    SELECT
        id	                                     AS	classifier_id,	
        created_at,		               
        dataset_id                               AS aiblock_id,	
        emoji,		
        hitl_setting,		
        is_enabled,	
        max_error_rate,	
        CAST(owner_id AS STRING)                 AS user_id,		
        project_id,	
        status,		
        updated_at,	
        valohai_deployment_id,
        _airbyte_emitted_at,	
        _airbyte_production_prediction_models_classifier_hashid 
    FROM 
        source

)

SELECT *
FROM renamed