WITH source AS (

    SELECT * FROM {{ source('public', 'production_datasets_data') }}

),

renamed AS (

    SELECT
        id                                   AS datapoint_id,
        CAST(created_at AS TIMESTAMP)        AS date_datapoint_uploaded,
        dataset_id                           AS aiblock_id, 
        original_file_name,  
        CAST(owner_id AS STRING)             AS user_id,
        remote_url,  
        storage_id,  
        text, 
        CAST(updated_at AS TIMESTAMP)        AS date_datapoint_updated,
        _airbyte_emitted_at,	
        _airbyte_production_datasets_data_hashid 
    FROM source

)

SELECT *
FROM renamed