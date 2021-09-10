SELECT 
    contact_id
FROM 
    {{ref("contact_enhanced")}}
WHERE
    contact_role="user"
    AND contact_email IS NULL