WITH email_confirmation_staging AS (
       
    SELECT 
        *
    FROM
        {{ref("email_confirmation_staging")}}

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
    email_confirmation_staging



