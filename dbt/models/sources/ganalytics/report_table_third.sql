WITH source AS (

    SELECT * FROM {{ source('google_cloud_function_google_analytics_report','report_table_third') }}

),

renamed AS (

    SELECT	
        social_network,	
        CAST(year_week AS STRING) AS year_week,	
        SUBSTRING(CAST(year_week AS STRING),5,6) AS week,
        _fivetran_batch,			
        _fivetran_deleted,			
        _fivetran_index,		
        _fivetran_synced,			
        users	 
    FROM source

)

SELECT *
FROM renamed