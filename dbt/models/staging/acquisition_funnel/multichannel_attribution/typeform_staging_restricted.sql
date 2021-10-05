WITH pages_view AS (
    
    SELECT 
        DISTINCT anonymous_id 
    FROM 
        {{ref("pages_view")}} 
    WHERE 
        user_id IS NULL
    
), sign_up_view AS (
    
    SELECT 
        DISTINCT anonymous_id 
    FROM 
        {{ref("sign_up_view")}} 
    
), segment_web_sessions__stitched AS (
       
    SELECT 
        *
    FROM
        {{ref("segment_web_sessions__stitched")}}
    WHERE 
        anonymous_id IN (SELECT * FROM pages_view)
        AND anonymous_id IN (SELECT * FROM sign_up_view)

), first_typeform AS (

    SELECT 
        anonymous_id,
        MIN(timestamp) AS first_typeform_date
    FROM
        {{ref("pages_view")}}
    WHERE 
        url LIKE "%getting-started-1%"
    GROUP BY
        anonymous_id

), threshold AS (
    
    SELECT 
        s.session_start_tstamp,
        s.session_end_tstamp,
        w.first_typeform_date,
        w.anonymous_id,                                                                               
        s.session_id,
        ROW_NUMBER() OVER (PARTITION by w.anonymous_id ORDER BY s.session_start_tstamp)               AS session_seq,
        CASE 
            WHEN (w.first_typeform_date BETWEEN s.session_start_tstamp AND s.session_end_tstamp) OR w.first_typeform_date=s.session_start_tstamp THEN true
          	ELSE false
        END                                                                                           AS conversion_session,
        CASE 
            WHEN w.first_typeform_date < s.session_start_tstamp THEN true
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
        segment_web_sessions__stitched s
    JOIN first_typeform  w
        ON CAST(w.anonymous_id AS string) = s.anonymous_id
    WHERE 
        w.first_typeform_date >= s.session_start_tstamp
    ORDER BY 
        w.anonymous_id, 
        s.session_start_tstamp

), model AS (
   
    SELECT 
        *,
        CASE 
            WHEN session_id = LAST_VALUE(session_id) OVER (PARTITION BY anonymous_id ORDER BY session_start_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) THEN 1
            ELSE 0
        END                                                                                                                                                                                 AS last_touch_attrib_pct,
        CASE 
            WHEN session_id = FIRST_VALUE(session_id) OVER (PARTITION BY anonymous_id ORDER BY session_start_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) THEN 1
            ELSE 0
        END                                                                                                                                                                                 AS first_touch_attrib_pct,
        1/COUNT(session_id) OVER (PARTITION BY anonymous_id)                                                                                                                                AS even_click_attrib_pct,
        CASE 
            WHEN session_start_tstamp >= timestamp_sub(first_typeform_date, interval 7 DAY)                                                                                               THEN 1
            WHEN session_start_tstamp > timestamp_sub(first_typeform_date, interval 14 DAY) AND session_start_tstamp < timestamp_sub(first_typeform_date, interval 7 DAY)       THEN .5
            WHEN session_start_tstamp > timestamp_sub(first_typeform_date, interval 21 DAY) AND session_start_tstamp > timestamp_sub(first_typeform_date, interval 14 DAY)      THEN .25
            WHEN session_start_tstamp > timestamp_sub(first_typeform_date, interval 28 DAY) AND session_start_tstamp > timestamp_sub(first_typeform_date, interval 21 DAY)      THEN .125
            ELSE 0
        END                                                                                                                                                                                 AS time_decay_attrib_pct,
        CASE 
            WHEN utm_medium="paid" THEN 1
            ELSE 0
        END                                                                                                                                                                                 AS is_paid,
        CASE 
            WHEN referrer IS NULL AND NOT(utm_medium="paid") THEN 1
            ELSE 0
        END                                                                                                                                                                                 AS is_direct,
        CASE 
            WHEN (referrer LIKE "%google%" OR referrer LIKE "%bing%" OR referrer LIKE "%yahoo%" OR referrer LIKE "%duck%") AND NOT(utm_medium="paid") THEN 1
            ELSE 0
        END                                                                                                                                                                                 AS is_SEO,
        CASE 
            WHEN (referrer LIKE "%facebook%" OR referrer LIKE "%t.co%" OR referrer LIKE "%linkedin%" OR referrer LIKE "%instagram%") AND NOT(utm_medium="paid") THEN 1
            ELSE 0
        END                                                                                                                                                                                 AS is_social,
        CASE 
            WHEN referrer LIKE "%levity%" OR referrer LIKE "%sendibt%" AND NOT(utm_medium="paid") THEN 1
            ELSE 0
        END                                                                                                                                                                                 AS is_internal
    FROM 
        threshold

)

SELECT 
    *,
    CASE 
        WHEN is_paid+is_social+is_SEO+is_internal+is_direct=0 THEN 1
        ELSE 0
    END                                                                         AS is_residual
FROM 
    model

