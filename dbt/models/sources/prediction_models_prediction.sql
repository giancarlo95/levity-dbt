WITH source AS (

    SELECT * FROM {{ source('google_cloud_postgresql_public', 'prediction_models_prediction') }}

), renamed AS (

    SELECT
        id	                        AS prediction_id,		
        _fivetran_deleted,		
        _fivetran_synced,			
        classifier_id,			    
        created_at                  AS date_prediction_made,		
        owner_id                    AS user_id,		
        updated_at			
	
    FROM 
        source

)

SELECT *
FROM renamed