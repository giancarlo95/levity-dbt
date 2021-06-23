WITH source AS (

    SELECT * FROM {{ source('public', 'production_prediction_models_prediction') }}

), renamed AS (

    SELECT
        id	                        AS prediction_id,					
        classifier_id,			    
        created_at                  AS date_prediction_made,		
        CAST(owner_id AS STRING)    AS user_id,		
        updated_at,
        _airbyte_emitted_at,	
        _airbyte_production_prediction_models_prediction_hashid 		
	
    FROM 
        source

)

SELECT *
FROM renamed