SELECT 
    user_email_address
FROM 
    {{ref("onboarded_users")}}
WHERE
    DATE_DIFF(CAST(date_user_onboarded AS DATE), DATE '2020-01-01', DAY)<0 
    OR DATE_DIFF(CAST(date_user_onboarded AS DATE), CURRENT_DATE(), DAY)>0