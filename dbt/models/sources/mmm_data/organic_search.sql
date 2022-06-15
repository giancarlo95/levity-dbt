WITH source AS (

    SELECT * FROM {{ source('google_search_console', 'clicks_impressions_by_page')}}

), renamed AS (

    SELECT
        *
    FROM 
        source
        
    {# ORDER BY
        year_month DESC #}

)

SELECT *
FROM renamed