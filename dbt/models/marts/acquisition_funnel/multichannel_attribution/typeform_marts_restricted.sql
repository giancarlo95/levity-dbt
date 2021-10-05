WITH typeform_staging_restricted AS (
       
    SELECT 
        *
    FROM
        {{ref("typeform_staging_restricted")}}

)

SELECT 
    *,
    DATE(first_typeform_date)                AS first_typeform_date_format,
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
    first_touch_attrib_pct*is_residual       AS residual_first_touch,
    last_touch_attrib_pct*is_paid            AS paid_last_touch,
    last_touch_attrib_pct*is_direct          AS direct_last_touch,
    last_touch_attrib_pct*is_SEO             AS SEO_last_touch,
    last_touch_attrib_pct*is_social          AS social_last_touch,
    last_touch_attrib_pct*is_internal        AS internal_last_touch,
    last_touch_attrib_pct*is_residual        AS residual_last_touch
FROM   
    typeform_staging_restricted



