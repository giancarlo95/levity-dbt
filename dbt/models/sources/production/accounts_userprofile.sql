WITH source AS (

    SELECT * FROM {{ source('google_cloud_postgresql_public', 'accounts_userprofile') }}

),

renamed AS (

    SELECT
        id,				
        is_approved,		
        is_service_account,		
        CAST(user_id AS STRING) AS user_id,
        --_airbyte_emitted_at,	
        --_airbyte_production_accounts_paymentplan_hashid
        _fivetran_deleted,
        _fivetran_synced  
    FROM source

)

SELECT *
FROM renamed
