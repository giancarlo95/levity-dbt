SELECT 
    contact_email
FROM 
    {{ref("contact_enhanced")}}
WHERE
    DATE_DIFF(CAST(signed_up_at AS DATE), DATE '2020-01-01', DAY)<0 
    OR DATE_DIFF(CAST(signed_up_at AS DATE), CURRENT_DATE(), DAY)>0