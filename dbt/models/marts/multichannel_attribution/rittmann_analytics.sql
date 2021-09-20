WITH segment_web_sessions__stitched AS (
       
    SELECT 
        *
    FROM
        {{ref("segment_web_sessions__stitched")}}

), first_email_confirmation AS (

    SELECT 
        anonymous_id,
        MIN(received_at) AS first_email_confirmation_date
    FROM
        {{ref("sign_up")}}
    GROUP BY
        anonymous_id

), threshold AS (
    
    SELECT 
        s.session_start_tstamp,
        s.session_end_tstamp,
        w.first_email_confirmation_date,
        w.anonymous_id,                                                                               
        s.session_id,
        ROW_NUMBER() OVER (PARTITION by w.anonymous_id ORDER BY s.session_start_tstamp)               AS session_seq,
        CASE 
            WHEN (w.first_email_confirmation_date BETWEEN s.session_start_tstamp AND s.session_end_tstamp) OR w.first_email_confirmation_date=s.session_start_tstamp THEN true
          	ELSE false
        END                                                                                           AS conversion_session,
        CASE 
            WHEN w.first_email_confirmation_date < s.session_start_tstamp THEN true
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
    JOIN first_email_confirmation  w
        ON CAST(w.anonymous_id AS string) = s.anonymous_id
    WHERE 
        w.first_email_confirmation_date >= s.session_start_tstamp
    ORDER BY 
        w.anonymous_id, 
        s.session_start_tstamp

), session_attribution AS (
   
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
            WHEN session_start_tstamp >= timestamp_sub(first_email_confirmation_date, interval 7 DAY)                                                                                               THEN 1
            WHEN session_start_tstamp > timestamp_sub(first_email_confirmation_date, interval 14 DAY) AND session_start_tstamp < timestamp_sub(first_email_confirmation_date, interval 7 DAY)       THEN .5
            WHEN session_start_tstamp > timestamp_sub(first_email_confirmation_date, interval 21 DAY) AND session_start_tstamp > timestamp_sub(first_email_confirmation_date, interval 14 DAY)      THEN .25
            WHEN session_start_tstamp > timestamp_sub(first_email_confirmation_date, interval 28 DAY) AND session_start_tstamp > timestamp_sub(first_email_confirmation_date, interval 21 DAY)      THEN .125
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
            WHEN (referrer LIKE "%google%" OR referrer LIKE "%bing%") AND NOT(utm_medium="paid") THEN 1
            ELSE 0
        END                                                                                                                                                                                 AS is_SEO,
        CASE 
            WHEN (referrer LIKE "%facebook%" OR referrer LIKE "%t.co%") AND NOT(utm_medium="paid") THEN 1
            ELSE 0
        END                                                                                                                                                                                 AS is_social,
        CASE 
            WHEN referrer LIKE "%levity%" AND NOT(utm_medium="paid") THEN 1
            ELSE 0
        END                                                                                                                                                                                 AS is_internal,
        CASE 
            WHEN referrer LIKE "%piexchange%" OR referrer LIKE "%read.aatt%" OR referrer LIKE "%avalanlabs%" THEN 1
            ELSE 0
        END                                                                                                                                                                                 AS is_residual
    FROM 
        threshold
    WHERE 
        NOT(entrance_url_path LIKE "%welcome%")

)

SELECT 
    *,
    even_click_attrib_pct*is_paid            AS paid_homogeneous,
    even_click_attrib_pct*is_direct          AS direct_homogeneous,
    even_click_attrib_pct*is_SEO             AS SEO_homogeneous,
    even_click_attrib_pct*is_social          AS social_homogeneous,
    even_click_attrib_pct*is_internal        AS internal_homogeneous,
    even_click_attrib_pct*is_residual        AS residual_homogeneous,
    first_touch_attrib_pct*is_paid           AS paid_first_touch,
    first_touch_attrib_pct*is_direct         AS direct_first_touch,
    first_touch_attrib_pct*is_SEO            AS SEO_first_touch,
    first_touch_attrib_pct*is_social         AS social_first_touch,
    first_touch_attrib_pct*is_internal       AS internal_first_touch,
    first_touch_attrib_pct*is_residual       AS residual_first_touch
FROM   
    session_attribution



