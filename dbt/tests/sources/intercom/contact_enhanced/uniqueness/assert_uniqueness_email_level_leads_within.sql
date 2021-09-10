SELECT 
    contact_email
FROM 
    {{ref("contact_enhanced")}}
WHERE
    contact_role="lead"
    AND contact_email IS NOT NULL
GROUP BY
    contact_email
HAVING
    COUNT(*)>1