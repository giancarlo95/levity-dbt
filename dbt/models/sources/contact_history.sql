WITH source AS (

    SELECT * FROM {{ source('intercom', 'contact_history') }}

),

renamed AS (

    SELECT
        id	                             AS contact_id,		
        updated_at                       AS date_contact_updated,			
        _fivetran_deleted,	
        _fivetran_synced,			
        created_at                       AS date_contact_created,				
        custom_signed_up_at              AS date_contact_signup_website,	
        email                            AS contact_email_address,				
        location_city,			
        location_country,			
        location_region,		
        name                             AS contact_name,
        role                             AS contact_role,				
        signed_up_at                     AS date_contact_signup_app	  
    FROM source

)

SELECT *
FROM renamed






	
