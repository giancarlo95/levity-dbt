WITH source AS (

    SELECT * FROM {{ source('google_sheets', 'typeform_first_step') }}

)

SELECT 			
    work_email,			
    email_other,
    CASE 
        WHEN work_email IS NULL THEN email_other
        ELSE work_email	
    END                                             AS email_address	
FROM 
    source