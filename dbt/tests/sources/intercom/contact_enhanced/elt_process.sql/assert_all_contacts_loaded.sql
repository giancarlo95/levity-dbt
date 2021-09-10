WITH contact_enhanced_raw AS (

    SELECT 
        * 
    FROM 
        {{ ref('contact_enhanced_raw') }}

), all_contacts AS (

    SELECT 
        *
    FROM 
        {{ ref('all_contacts') }}

), residual AS (

    SELECT contact_id FROM all_contacts EXCEPT DISTINCT
    SELECT contact_id FROM contact_enhanced_raw 

) 

SELECT 
    residual.contact_id,
    all_contacts.contact_email
FROM residual LEFT JOIN all_contacts ON all_contacts.contact_id=residual.contact_id