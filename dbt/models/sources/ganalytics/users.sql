WITH source AS (

    SELECT * FROM {{ source('google_analytics','users') }}

),

renamed AS (

    SELECT
        _fivetran_id,		
        date,			
        profile,		
        _7_day_users            AS previous_week_users,		
        _fivetran_synced	 
    FROM source

)

SELECT *
FROM renamed
