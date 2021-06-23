WITH source AS (

    SELECT * FROM {{ source('intercom', 'contact_tag_history') }}

),

renamed AS (

    SELECT
        contact_id,		
        contact_updated_at            AS date_contact_updated,		
        tag_id,	
        _fivetran_synced 
    FROM source

)

SELECT *
FROM renamed