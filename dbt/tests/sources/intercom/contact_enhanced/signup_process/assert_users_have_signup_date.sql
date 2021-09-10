SELECT 
    contact_email
FROM 
    {{ref("contact_enhanced")}}
WHERE
    contact_role="user"
    AND signed_up_at IS NULL
