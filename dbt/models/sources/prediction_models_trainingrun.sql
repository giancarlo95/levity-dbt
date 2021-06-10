WITH source AS (

    SELECT * FROM {{ source('google_cloud_postgresql_public', 'prediction_models_trainingrun')}}

), renamed AS (

    SELECT
        id                          AS training_id,		
        _fivetran_deleted,	
        _fivetran_synced,	
        classifier_version_id       AS version_id,	
        created_at                  AS date_training_run,	
        owner_id                    AS user_id,
        updated_at	
    FROM 
        source

)

SELECT *
FROM renamed