WITH source AS (

    SELECT * FROM {{ source('public', 'production_prediction_models_trainingrun')}}

), renamed AS (

    SELECT
        id                                          AS training_id,			
        classifier_version_id                       AS version_id,	
        CAST(created_at AS TIMESTAMP)               AS date_training_run,	
        CAST(owner_id AS STRING)                    AS user_id,
        CAST(updated_at AS TIMESTAMP)               AS date_training_updated,
        _airbyte_emitted_at,	
        _airbyte_production_prediction_models_trainingrun_hashid 	
    FROM 
        source

)

SELECT *
FROM renamed