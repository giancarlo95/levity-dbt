WITH source AS (

    SELECT * FROM {{ source('google_cloud_function_frontegg','tenant_metadata_table') }}

),

renamed AS (

    SELECT
        --_fivetran_batch,			
        --_fivetran_index,			
        --_fivetran_synced,			
        email_valid,			
        id,			
        is_approved,		
        owner_email,			
        owner_id,			
        tenant_logo_url										 
    FROM source

)

SELECT *
FROM renamed