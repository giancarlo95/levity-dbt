WITH contact_enhanced AS (

    SELECT 
        contact_id,
        contact_email,
        contact_role,
        CASE 
            WHEN signed_up_at IS NULL THEN created_at
            ELSE signed_up_at 
        END                                 AS signed_up_at,
        all_contact_tags,
        CASE 
            WHEN all_contact_tags LIKE "%Company: Levity%" THEN 1
            ELSE 0 
        END                                 AS internal_user
    FROM 
       {{ref('contact_enhanced')}}
    

), typeform_first_step AS (

    SELECT 
        email_address
    FROM 
       {{ref('typeform_first_step')}}

), transformed AS (

    SELECT 
        contact_id,
        contact_email,
        EXTRACT(YEAR FROM signed_up_at)                        AS year,
        EXTRACT(WEEK FROM signed_up_at)                        AS week_number,
        LAST_DAY(CAST(signed_up_at AS DATE), WEEK)             AS week_end
    FROM 
        contact_enhanced
    WHERE 
        internal_user=0
        AND contact_role="user"

) 

SELECT 
    year,
    week_number,
    week_end,
    COUNT(contact_id)                                      AS number_of_forms
FROM 
    transformed
INNER JOIN typeform_first_step 
    ON typeform_first_step.email_address=transformed.contact_email 
GROUP BY
    year, 
    week_number,
    week_end