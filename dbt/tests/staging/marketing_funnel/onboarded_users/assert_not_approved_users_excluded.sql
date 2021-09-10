SELECT 
    user_email_address
FROM 
    {{ref('onboarded_users')}} a
INNER JOIN 
    {{ref('accounts_userprofile')}} b ON b.user_id=a.old_user_id AND b.is_approved=false