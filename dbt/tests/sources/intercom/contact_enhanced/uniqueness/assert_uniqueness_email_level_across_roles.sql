WITH users AS (
    
    SELECT 
        DISTINCT contact_email
    FROM 
        {{ref("contact_enhanced")}}
    WHERE
        contact_role="user"
        AND contact_email IS NOT NULL

), leads AS (
    
    SELECT 
        DISTINCT contact_email
    FROM 
        {{ref("contact_enhanced")}}
    WHERE
        contact_role="lead" 
        AND contact_email IS NOT NULL

)

SELECT 
    users.contact_email
FROM users 
INNER JOIN leads ON users.contact_email=leads.contact_email

