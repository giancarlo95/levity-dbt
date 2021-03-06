WITH source AS (

    SELECT * FROM {{ source('public', 'datasets_datalabel') }}

),

renamed AS (

    SELECT
        id                                        AS aiblock_id,					
        bounding_box_id,		
        classifier_version_id,			
        confidence_score,		
        CAST(created_at AS TIMESTAMP)             AS date_labelled_datapoint_uploaded,			
        data_id                                   AS datapoint_id,			
        label_id,			
        CAST(owner_id AS STRING)                  AS old_user_id,
        frontegg_user_id                          AS user_id,
        frontegg_tenant_id                        AS account_id,
        task_action_id,			
        CAST(updated_at AS TIMESTAMP)             AS date_labelled_datapoint_updated		
    FROM source

)

SELECT *
FROM renamed