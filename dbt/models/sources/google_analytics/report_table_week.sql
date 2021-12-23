WITH source AS (

    SELECT * FROM {{ source('google_cloud_function_google_analytics_report', 'report_table_week')}}

), renamed AS (

    SELECT
        *
    FROM 
        source
    ORDER BY
        year_week DESC

)

SELECT *
FROM renamed