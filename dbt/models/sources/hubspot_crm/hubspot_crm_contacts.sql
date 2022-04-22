{{
  config(
    materialized = 'table',
    )
}}

WITH source AS (

    SELECT * FROM {{ source('hubspot_crm', 'contacts_view')}}

), renamed AS (

    SELECT
        *
    FROM 
        source

)

SELECT *
FROM renamed