WITH source AS (

    SELECT * FROM {{ source('public', 'production_prediction_models_prediction') }}

), renamed AS (

    SELECT
        id	                                          AS prediction_id,					
        classifier_id,			    
        CAST(created_at AS TIMESTAMP)                 AS date_prediction_made,		
        CAST(owner_id AS STRING)                      AS old_user_id,
        frontegg_user_id                              AS user_id,
        frontegg_tenant_id                            AS account_id,		
        CAST(updated_at AS TIMESTAMP)                 AS date_prediction_updated,
        _airbyte_emitted_at,	
        _airbyte_production_prediction_models_prediction_hashid 		
	
    FROM 
        source

)

SELECT *
FROM renamed