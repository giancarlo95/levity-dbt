WITH source AS (

    SELECT * FROM {{ source('google_cloud_function_intercom_contacts', 'contact_table') }}

)

SELECT 
    contact_id,			
    _fivetran_batch,		
    _fivetran_deleted,			
    _fivetran_index,			
    _fivetran_synced,			
    contact_email,			
    created_at			
FROM 
    source
WHERE 
    DATE(_fivetran_synced)=CURRENT_DATE()

