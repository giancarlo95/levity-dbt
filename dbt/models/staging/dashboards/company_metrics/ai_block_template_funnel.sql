{{
  config(
    materialized = 'table',
    )
}}

WITH workspace_onboarded AS (

    SELECT 
        workspace_id,
        email,
        onboarded_at,
        CAST(onboarded_at AS STRING) AS onboarded_at_string
    FROM
        {{ref("subscriptions")}} uo

), 

cloned_template AS (

    SELECT 
        *
    FROM
        {{ref("a_cloned_template_workspace")}}

), 

retrained_model AS (

    SELECT 
        *
    FROM
        {{ref("d_retrained_model_workspace")}} 

), 

made_test_pred AS (

    SELECT 
        *
    FROM
        {{ref("e_made_test_pred_template_workspace")}}

), 

made_prod_pred AS (

    SELECT 
        *
    FROM
        {{ref("f_made_prod_pred_template_workspace")}}

), 

made_50_prod_pred AS (

    SELECT 
        *
    FROM
        {{ref("g_made_50_prod_pred_template_workspace")}}

),

joined AS (

    SELECT
        EXTRACT(YEAR FROM onboarded_at) AS year,
        EXTRACT(MONTH FROM onboarded_at) AS month,
        FORMAT_TIMESTAMP("%b %Y", onboarded_at) AS year_month,
        *,
        TIMESTAMP_DIFF(cloned_template_at, onboarded_at, MINUTE) AS reached_cloned_template_minutes,
        TIMESTAMP_DIFF(retrained_model_at, onboarded_at, MINUTE) AS reached_retrained_model_minutes,
        TIMESTAMP_DIFF(made_test_pred_at, onboarded_at, MINUTE) AS reached_made_test_pred_minutes,
        TIMESTAMP_DIFF(made_prod_pred_at, onboarded_at, MINUTE) AS reached_made_prod_pred_minutes,
        TIMESTAMP_DIFF(made_50_prod_pred_at, onboarded_at, MINUTE) AS reached_made_50_prod_pred_minutes
    FROM
        workspace_onboarded wo
    LEFT JOIN cloned_template USING(workspace_id)
    LEFT JOIN retrained_model USING(workspace_id)
    LEFT JOIN made_test_pred USING(workspace_id)
    LEFT JOIN made_prod_pred USING(workspace_id)
    LEFT JOIN made_50_prod_pred USING(workspace_id)
    WHERE
        NOT(wo.email LIKE "%@levity.ai")

), 

median_workaround AS (

    SELECT
        year_month,
        year,
        month, 
        ANY_VALUE(reached_cloned_template_minutes_mdn) AS reached_cloned_template_minutes_mdn,
        ANY_VALUE(reached_retrained_model_minutes_mdn) AS reached_retrained_model_minutes_mdn,
        ANY_VALUE(reached_made_test_pred_minutes_mdn) AS reached_made_test_pred_minutes_mdn,
        ANY_VALUE(reached_made_prod_pred_minutes_mdn) AS reached_made_prod_pred_minutes_mdn,
        ANY_VALUE(reached_made_50_prod_pred_minutes_mdn) AS reached_made_50_prod_pred_minutes_mdn
    FROM
    (SELECT
        year,
        month,
        year_month,
        PERCENTILE_CONT(reached_cloned_template_minutes, 0.5) OVER(PARTITION BY year_month) AS reached_cloned_template_minutes_mdn,
        PERCENTILE_CONT(reached_retrained_model_minutes, 0.5) OVER(PARTITION BY year_month) AS reached_retrained_model_minutes_mdn,
        PERCENTILE_CONT(reached_made_test_pred_minutes, 0.5) OVER(PARTITION BY year_month) AS reached_made_test_pred_minutes_mdn,
        PERCENTILE_CONT(reached_made_prod_pred_minutes, 0.5) OVER(PARTITION BY year_month) AS reached_made_prod_pred_minutes_mdn,
        PERCENTILE_CONT(reached_made_50_prod_pred_minutes, 0.5) OVER(PARTITION BY year_month) AS reached_made_50_prod_pred_minutes_mdn
    FROM
        joined) AS partitioned
    GROUP BY
        1,
        2,
        3

)

SELECT 
    year_month,
    year,
    month,
    COUNT(*) AS onboarded_count,
    COUNT(CASE WHEN cloned_template_at IS NOT NULL THEN 1 END) AS reached_cloned_template_count,
    AVG(reached_cloned_template_minutes) AS reached_cloned_template_minutes_avg,
    ANY_VALUE(reached_cloned_template_minutes_mdn) AS reached_cloned_template_minutes_mdn,
    COUNT(CASE WHEN retrained_model_at IS NOT NULL THEN 1 END) AS reached_retrained_model_count,
    AVG(reached_retrained_model_minutes) AS reached_retrained_model_minutes_avg,
    ANY_VALUE(reached_retrained_model_minutes_mdn) AS reached_retrained_model_minutes_mdn,
    COUNT(CASE WHEN made_test_pred_at IS NOT NULL THEN 1 END) AS reached_made_test_pred_count,
    AVG(reached_made_test_pred_minutes) AS reached_made_test_pred_minutes_avg,
    ANY_VALUE(reached_made_test_pred_minutes_mdn) AS reached_made_test_pred_minutes_mdn,
    COUNT(CASE WHEN made_prod_pred_at IS NOT NULL THEN 1 END) AS reached_made_prod_pred_count,
    AVG(reached_made_prod_pred_minutes) AS reached_made_prod_pred_minutes_avg,
    ANY_VALUE(reached_made_prod_pred_minutes_mdn) AS reached_made_prod_pred_minutes_mdn,
    COUNT(CASE WHEN made_50_prod_pred_at IS NOT NULL THEN 1 END) AS reached_made_50_prod_pred_count,
    AVG(reached_made_50_prod_pred_minutes) AS reached_made_50_prod_pred_minutes_avg,
    ANY_VALUE(reached_made_50_prod_pred_minutes_mdn) AS reached_made_50_prod_pred_minutes_mdn
FROM    
    joined
LEFT JOIN median_workaround USING (year_month, year, month)
GROUP BY
    1,
    2,
    3
ORDER BY
    2 ASC,
    3 ASC





