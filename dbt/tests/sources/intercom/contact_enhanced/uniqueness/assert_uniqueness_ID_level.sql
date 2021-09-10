SELECT 
    contact_id
FROM 
    {{ref("contact_enhanced")}}
GROUP BY
    contact_id
HAVING
    COUNT(*)>1


