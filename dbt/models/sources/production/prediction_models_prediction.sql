WITH source1 AS (

    SELECT * FROM {{ source('public', 'prediction_models_prediction') }}

), renamed AS (

    SELECT
        id	                                          AS prediction_id,
        hitl                                          AS is_hitl,
        source                                        AS origin,
        workflow_id,					
        classifier_id,			    
        CAST(created_at AS TIMESTAMP)                 AS date_prediction_made,		
        CAST(owner_id AS STRING)                      AS old_user_id,
        frontegg_user_id                              AS user_id,
        frontegg_tenant_id                            AS workspace_id,		
        CAST(updated_at AS TIMESTAMP)                 AS date_prediction_updated,	
    FROM 
        source1

)

SELECT *
FROM renamed