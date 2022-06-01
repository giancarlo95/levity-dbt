{{
  config(
    materialized = 'table',
    )
}}

WITH source AS (

    SELECT * FROM {{ source('labs', 'vetevo')}}

), renamed AS (

    SELECT
        frontegg_tenant_id             AS workspace_id,
        image_count                    AS predictions_count,
        CAST(created_at AS DATE)       AS date_prediction_made 	
    FROM 
        source

)

SELECT *
FROM renamed