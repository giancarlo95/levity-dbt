WITH source AS (

    SELECT * FROM {{ source('core_analytics_prod', 'changeEvents')}}

), renamed AS (

    SELECT
        s.table AS table_name,
        s.created_at AS created_at,
        s.id AS event_id,
        s.op,
        s.data AS content
    FROM 
        source s

)

SELECT *
FROM renamed