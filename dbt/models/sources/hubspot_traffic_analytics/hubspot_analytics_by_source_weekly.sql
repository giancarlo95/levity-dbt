WITH source AS (

    SELECT * FROM {{ source('hubspot_traffic_analytics', 'hubspot_analytics_by_source_weekly')}}

), renamed AS (

    SELECT
        *
    FROM 
        source

)

SELECT *
FROM renamed