WITH segment_web_sessions AS (

    SELECT 
        * 
    FROM 
        {{ref('segment_web_sessions')}} 

), onboarded_user_ids AS (

    SELECT 
        * 
    FROM 
        {{ref('onboarded_user_ids')}} 

),  onboarded_users AS (

    SELECT 
        * 
    FROM 
        {{ref('onboarded_users')}} 

), segment_web_sessions_users AS (

    SELECT 
        blended_user_id
    FROM 
        {{ref('segment_web_sessions')}} 

), user_match AS (

    SELECT 
        blended_user_id,
        a.user_id,
        b.old_user_id,
        a.user_email_address                      AS email1,
        b.user_email_address                      AS email2
    FROM
        segment_web_sessions_users
    LEFT JOIN onboarded_user_ids AS a ON segment_web_sessions_users.blended_user_id=a.user_id
    LEFT JOIN onboarded_user_ids AS b ON segment_web_sessions_users.blended_user_id=b.old_user_id

) 

SELECT
    CASE
        WHEN blended_user_id IS NOT NULL AND blended_user_id=user_match.user_id THEN blended_user_id
        WHEN NOT(blended_user_id=user_match.user_id) AND old_user_id IS NOT NULL THEN onboarded_users.user_id 
    END AS blended_user_id,
    user_match.user_id,
    user_match.old_user_id,
    email1,
    email2
FROM 
    user_match
LEFT JOIN onboarded_users ON user_match.email2=onboarded_users.user_email_address


