WITH contact_enhanced AS (

    SELECT 
        contact_id,
        contact_email,
        signed_up_at,
        all_contact_tags
    FROM 
       {{ref('contact_enhanced')}}
    WHERE 
        NOT(contact_email LIKE '%levity%')

)

SELECT 
    EXTRACT(YEAR FROM signed_up_at)                        AS year,
    EXTRACT(WEEK FROM signed_up_at)                        AS week_number,
    LAST_DAY(CAST(signed_up_at AS DATE), WEEK)             AS week_end,
    COUNT(contact_id)                                      AS number_of_access_call_booked
FROM 
    contact_enhanced
WHERE 
    all_contact_tags LIKE '%Access Call booked%'
GROUP BY
    EXTRACT(YEAR FROM signed_up_at),
    EXTRACT(WEEK FROM signed_up_at),
    LAST_DAY(CAST(signed_up_at AS DATE), WEEK)                