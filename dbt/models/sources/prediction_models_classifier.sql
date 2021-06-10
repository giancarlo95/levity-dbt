WITH source AS (

    SELECT * FROM {{ source('google_cloud_postgresql_public', 'prediction_models_classifier') }}

), renamed AS (

    SELECT
        id	                     AS	classifier_id,
        _fivetran_deleted,		
        _fivetran_synced,		
        created_at,		
        dataset_id               AS aiblock_id,	
        emoji,		
        hitl_setting,		
        is_enabled,	
        max_error_rate,	
        owner_id                 AS user_id,		
        project_id,	
        status,		
        updated_at,	
        valohai_deployment_id
    FROM 
        source

)

SELECT *
FROM renamed