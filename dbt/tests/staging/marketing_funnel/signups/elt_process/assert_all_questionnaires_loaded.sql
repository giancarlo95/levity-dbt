WITH contact_enhanced_raw AS (

    SELECT 
        contact_email              AS email_address
    FROM 
        {{ref('contact_enhanced_raw')}}
    WHERE 
        contact_email IS NOT NULL

), typeform_first_step AS (

    SELECT 
        LOWER(email_address)       AS email_address
    FROM 
       {{ref('typeform_first_step')}}
    WHERE 
        NOT(email_address LIKE "%@levity.ai%")

), all_contacts AS (

    SELECT 
        contact_email              AS email_address
    FROM 
        {{ ref('all_contacts') }}

), residual AS (

    SELECT email_address FROM typeform_first_step EXCEPT DISTINCT
    SELECT email_address FROM contact_enhanced_raw

)

SELECT 
    DISTINCT residual.email_address
FROM residual 
INNER JOIN all_contacts ON all_contacts.email_address=residual.email_address

