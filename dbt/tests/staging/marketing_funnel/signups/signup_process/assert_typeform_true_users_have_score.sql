SELECT 
    contact_email
FROM 
    {{ref("signups")}}
WHERE 
    typeform=true
    AND contact_role="user"
    AND NOT(score IN ("high score", "low score"))

