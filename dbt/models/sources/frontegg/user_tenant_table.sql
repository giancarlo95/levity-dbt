WITH source AS (

    SELECT * FROM {{ source('google_cloud_function_frontegg','user_tenant_table') }}

),

renamed AS (

    SELECT
        _fivetran_batch,			
        _fivetran_index,			
        _fivetran_synced,			
        frontegg_tenant_id               AS account_id,			
        frontegg_user_id                 AS user_id								 
    FROM source

)

SELECT *
FROM renamed