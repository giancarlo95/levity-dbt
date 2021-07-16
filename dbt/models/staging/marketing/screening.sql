WITH contact_last AS (

    SELECT * FROM {{ref('contact_last')}}

), contact_tag_last AS (

    SELECT * FROM {{ref('contact_tag_last')}}

), contact_tag_last_1 AS (

    SELECT * FROM {{ref('contact_tag_last')}}

)

SELECT 
    contact_last.contact_id,
    contact_last.contact_email_address,
    contact_tag_last.tag_description              AS descr,
    contact_tag_last_1.tag_description            AS descr_1
FROM contact_last
INNER JOIN contact_tag_last 
    ON contact_tag_last.contact_id=contact_last.contact_id
    AND contact_tag_last.tag_description='change of sign up process'
LEFT JOIN contact_tag_last_1 
    ON contact_tag_last_1.contact_id=contact_last.contact_id
    AND contact_tag_last_1.tag_description='new sign up process API'
