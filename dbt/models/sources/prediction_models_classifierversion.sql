WITH source AS (

    SELECT * FROM {{ source('google_cloud_postgresql_public', 'prediction_models_classifierversion') }}

), renamed AS (

    SELECT
        id                           AS version_id,		
        _fivetran_deleted,		
        _fivetran_synced,		
        classifier_id,		
        created_at,		
        default,	
        evaluation,		
        fake,		
        owner_id                      AS user_id,		
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
        valohai_version_name	
    FROM 
        source

)

SELECT *
FROM renamed