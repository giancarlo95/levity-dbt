WITH source AS (

    SELECT * FROM {{ source('public', 'production_datasets_datalabel') }}

),

renamed AS (

    SELECT
        id,					
        bounding_box_id,		
        classifier_version_id,			
        confidence_score,		
        CAST(created_at AS TIMESTAMP)             AS date_labelled_datapoint_uploaded,			
        data_id                                   AS datapoint_id,			
        label_id,			
        CAST(owner_id AS STRING)                  AS user_id,	
        task_action_id,			
        CAST(updated_at AS TIMESTAMP)             AS date_labelled_datapoint_updated,
        _airbyte_emitted_at,	
        _airbyte_production_datasets_datalabel_hashid 			
    FROM source

)

SELECT *
FROM renamed