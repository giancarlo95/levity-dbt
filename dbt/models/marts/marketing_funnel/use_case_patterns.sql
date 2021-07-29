WITH  contact_enhanced AS (

    SELECT 
        contact_id,
        signed_up_at,
        CASE 
            WHEN custom_type_of_data LIKE 'Image%' THEN 'Image' 
            WHEN custom_type_of_data LIKE 'Document%' THEN 'Document' 
            WHEN custom_type_of_data LIKE 'Text%' THEN 'Text'
            WHEN custom_type_of_data LIKE 'Structured%' THEN 'Tabular Data'
            WHEN custom_type_of_data LIKE 'Video%' THEN 'Video or Sound'
            WHEN custom_type_of_data IS NULL THEN 'Missing'
            ELSE 'Other'
        END AS data_type
    FROM {{ref('contact_enhanced')}}

)

SELECT 
    *
FROM
    contact_enhanced
