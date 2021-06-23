WITH source AS (

    SELECT * FROM {{ source('public', 'production_datasets_datalabel') }}

),

renamed AS (

    SELECT
        id,					
        bounding_box_id,		
        classifier_version_id,			
        confidence_score,		
        created_at                                AS date_labelled_datapoint_uploaded,			
        data_id                                   AS datapoint_id,			
        label_id,			
        CAST(owner_id AS STRING)                  AS user_id,	
        task_action_id,			
        updated_at,
        _airbyte_emitted_at,	
        _airbyte_production_datasets_datalabel_hashid 			
    FROM source

)

SELECT *
FROM renamed