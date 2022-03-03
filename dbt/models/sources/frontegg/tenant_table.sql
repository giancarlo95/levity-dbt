{{
  config(
    materialized = 'table',
    )
}}

WITH source AS (

    SELECT * FROM {{ source('google_cloud_function_frontegg','tenant_table') }}

),

renamed AS (

    SELECT
        id,			
        _fivetran_batch,			
        _fivetran_deleted,			
        _fivetran_index,			
        _fivetran_synced,			
        created_at                           AS date_account_created,			
        frontegg_tenant_id                   AS account_id,			
        frontegg_vendor_id,			
        name                                 AS account_name,			
        updated_at                           AS date_account_updated						 
    FROM source

)

SELECT *
FROM renamed