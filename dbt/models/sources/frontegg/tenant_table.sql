WITH source AS (

    SELECT * FROM {{ source('google_cloud_function_frontegg','tenant_table') }}

),

renamed AS (

    SELECT
        id,			
        --_fivetran_batch,			
        --_fivetran_deleted,			
        --_fivetran_index,			
        DATE(_fivetran_synced)               AS date_fivetran_synced,			
        created_at,                           			
        frontegg_tenant_id                   AS workspace_id,			
        frontegg_vendor_id                   AS vendor_id,			
        name                                 AS workspace_name,			
        updated_at                           						 
    FROM source

)

SELECT 
    *,
    CASE 
        WHEN date_fivetran_synced=CURRENT_DATE() THEN 0
        ELSE 1
    END AS is_deleted
FROM renamed