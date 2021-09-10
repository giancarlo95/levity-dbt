SELECT 
    contact_email
FROM 
    {{ref("contact_enhanced")}}
WHERE
    contact_email LIKE "%@levity.ai%"
    