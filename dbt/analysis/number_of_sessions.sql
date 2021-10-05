WITH typeform_staging_restricted AS (

    SELECT
        *
    FROM
        {{ref("typeform_staging_restricted")}}
    
)

SELECT 
    anonymous_id,
    COUNT(*)             AS number_of_sessions,
    AVG(page_views)      AS number_of_page_views_per_session,
    AVG(duration_in_s)   AS duration_per_session
FROM 
    typeform_staging_restricted
GROUP BY 
    anonymous_id

