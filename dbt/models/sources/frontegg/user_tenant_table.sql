WITH source AS (

    SELECT * FROM {{ source('google_cloud_function_frontegg','user_tenant_table') }}

),

renamed AS (

    SELECT
        --_fivetran_batch,
        --_fivetran_deleted,			
        --_fivetran_index,			
        DATE(_fivetran_synced)           AS date_fivetran_synced,		
        frontegg_tenant_id               AS workspace_id,			
        frontegg_user_id                 AS user_id								 
    FROM source

)

SELECT 
    *,
    CASE 
        WHEN date_fivetran_synced=CURRENT_DATE() THEN 0
        ELSE 1
    END AS is_deleted
FROM renamed