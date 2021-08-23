WITH source AS (

    SELECT * FROM {{ source('google_cloud_function_google_analytics_report','report_table') }}

),

renamed AS (

    SELECT
        medium,			
        year_week,	
        _fivetran_batch,			
        _fivetran_deleted,			
        _fivetran_index,		
        _fivetran_synced,			
        users	 
    FROM source

)

SELECT *
FROM renamed