WITH source AS (

    SELECT * FROM {{ source('google_cloud_postgresql_public', 'datasets_data') }}

),

renamed AS (

    SELECT
        id                     AS datapoint_id,
        _fivetran_deleted,      
        _fivetran_synced, 
        created_at             AS date_datapoint_uploaded,
        dataset_id             AS aiblock_id, 
        original_file_name,  
        owner_id               AS user_id,
        remote_url,  
        storage_id,  
        text, 
        updated_at
    FROM source

)

SELECT *
FROM renamed