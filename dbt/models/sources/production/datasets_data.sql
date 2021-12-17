WITH source AS (

    SELECT * FROM {{ source('public', 'datasets_data') }}

),

renamed AS (

    SELECT
        id                                   AS datapoint_id,
        CAST(created_at AS TIMESTAMP)        AS date_datapoint_uploaded,
        dataset_id                           AS aiblock_id, 
        original_file_name,  
        CAST(owner_id AS STRING)             AS old_user_id,
        frontegg_user_id                     AS user_id,
        frontegg_tenant_id                   AS account_id,
        remote_url,  
        storage_id,  
        text, 
        CAST(updated_at AS TIMESTAMP)        AS date_datapoint_updated
    FROM source

)

SELECT *
FROM renamed