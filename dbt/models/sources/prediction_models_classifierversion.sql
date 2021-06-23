WITH source AS (

    SELECT * FROM {{ source('public', 'production_prediction_models_classifierversion') }}

), renamed AS (

    SELECT
        id                                            AS version_id,			
        classifier_id,		
        created_at,		
        --default,	
        evaluation,		
        fake,		
        CAST(owner_id AS STRING)                      AS user_id,		
        performance_score,		
        seconds_left,		
        status,	
        template,		
        training_progress,		
        updated_at,		
        valohai_endpoint_id,	
        valohai_endpoint_url,	
        valohai_execution_id,	
        valohai_version_id,	
        valohai_version_name,
        _airbyte_emitted_at,	
        _airbyte_production_prediction_models_classifierversion_hashid 
    FROM 
        source

)

SELECT *
FROM renamed