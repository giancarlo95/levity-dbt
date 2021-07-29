WITH  contact_enhanced AS (

    SELECT 
        contact_id,
        signed_up_at,
        CASE 
            WHEN custom_type_of_data IS NULL THEN 'Missing'
            ELSE 'Other'
        END AS data_type
    FROM {{ref('contact_enhanced')}}

)

SELECT 
    *
FROM
    contact_enhanced