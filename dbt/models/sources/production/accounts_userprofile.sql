WITH source AS (

    SELECT * FROM {{ source('public', 'accounts_userprofile') }}

),

renamed AS (

    SELECT
        id,				
        is_approved,		
        is_service_account,		
        CAST(user_id AS STRING) AS user_id 
    FROM source

)

SELECT *
FROM renamed
