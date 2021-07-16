WITH contact_tag_history AS (

    SELECT * FROM {{ref('contact_tag_history')}}

), contact_tag_history_last AS (

    SELECT 
        contact_id,
        MAX(date_contact_updated) as date_contact_last_updated
    FROM {{ref('contact_tag_history')}}
    GROUP BY
        contact_id

), tag AS (

    SELECT * FROM {{ref('tag')}}

), contact_last AS (

    SELECT * FROM {{ref('contact_last')}}

)

SELECT
	contact_tag_history.contact_id,
    contact_last.contact_email_address,
    contact_tag_history.tag_id,
    tag.tag_description
FROM contact_tag_history
INNER JOIN contact_tag_history_last
    ON contact_tag_history.contact_id=contact_tag_history_last.contact_id
    AND contact_tag_history.date_contact_updated=contact_tag_history_last.date_contact_last_updated
INNER JOIN tag 
    ON contact_tag_history.tag_id=tag.tag_id
    AND tag.tag_description in ('change of sign up process', 'new sign up process API')
INNER JOIN contact_last
    ON contact_tag_history.contact_id=contact_last.contact_id