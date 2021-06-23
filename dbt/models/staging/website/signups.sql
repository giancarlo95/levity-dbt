WITH contact_history AS (

    SELECT * FROM {{ref('contact_history')}}

), contact_history_last AS (

    SELECT 
        contact_id,
        MAX(date_contact_updated) as date_contact_last_updated
    FROM {{ref('contact_history')}}
    GROUP BY
        contact_id

)

SELECT
	contact_history.contact_id,
    contact_history.contact_email_address, 
    contact_history.contact_name,
    contact_history.contact_role,   
    contact_history.date_contact_created,
    contact_history.date_contact_signup_website,
    contact_history.date_contact_signup_app
FROM contact_history
INNER JOIN contact_history_last
    ON contact_history.contact_id=contact_history_last.contact_id
    AND contact_history.date_contact_updated=contact_history_last.date_contact_last_updated
WHERE
    contact_history.date_contact_signup_website IS NOT NULL 
    OR contact_history.date_contact_signup_app IS NOT NULL
