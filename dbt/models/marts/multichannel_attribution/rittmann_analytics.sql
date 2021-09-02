SELECT 
    w.user_email_address,
    s.session_start_tstamp,
    s.session_end_tstamp,
    w.date_user_onboarded,
    w.user_id                                                                                     AS user_id,
    s.session_id,
    ROW_NUMBER() OVER (PARTITION by w.user_id ORDER BY s.session_start_tstamp)                    AS session_seq,
    CASE 
        WHEN w.date_user_onboarded BETWEEN s.session_start_tstamp AND s.session_end_tstamp THEN true
      	ELSE false
    END                                                                                           AS conversion_session,
    CASE 
        WHEN w.date_user_onboarded < s.session_start_tstamp THEN true
        ELSE false
    END                                                                                           AS prospect_session,
    COALESCE(s.utm_source,'Direct') utm_source, 
    COALESCE(s.utm_content,'Direct') utm_content, 
    COALESCE(s.utm_medium,'Direct') utm_medium, 
    COALESCE(s.utm_campaign,'Direct') utm_campaign,
    s.first_page_url_path                                                                         AS entrance_url_path,
    s.last_page_url_path                                                                          AS exit_url_path,
    referrer,
    duration_in_s,
    page_views
FROM 
    {{ref("segment_web_sessions__stitched")}} s
JOIN {{ref("onboarded_users")}}  w
    ON CAST(w.user_id AS string) = s.blended_user_id
WHERE 
    w.date_user_onboarded >= s.session_start_tstamp
ORDER BY 
    w.user_id, s.session_start_tstamp