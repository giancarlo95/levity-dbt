WITH source AS (

    SELECT * FROM {{ source('google_cloud_function_google_analytics_report_monthly', 'report_table_month')}}

), renamed AS (

    SELECT
        *
    FROM 
        source
    ORDER BY
        year_month DESC

)

SELECT *
FROM renamed