{{
  config(
    materialized = 'table',
    )
}}

WITH workspace_onboarded AS (

    SELECT 
        context_group_id AS workspace_id,
        email,
        MIN(uo.timestamp) AS workspace_onboarded_at
    FROM
        {{ref("django_production_user_onboarded")}} uo
    GROUP BY
        context_group_id,
        email

), 

cloned_template AS (

    SELECT 
        *
    FROM
        {{ref("a_cloned_ai_block_template_workspace")}}

), 

retrained_model AS (

    SELECT 
        *
    FROM
        {{ref("d_possibly_retrained_model_workspace")}} 

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
        EXTRACT(YEAR FROM workspace_onboarded_at) AS year,
        EXTRACT(MONTH FROM workspace_onboarded_at) AS month,
        FORMAT_TIMESTAMP("%b %Y", workspace_onboarded_at) AS year_month,
        *,
        TIMESTAMP_DIFF(template_cloned_at, workspace_onboarded_at, MINUTE) AS reached_template_cloned_minutes,
        TIMESTAMP_DIFF(retrained_model_at, workspace_onboarded_at, MINUTE) AS reached_retrained_model_minutes,
        TIMESTAMP_DIFF(made_test_pred_at, workspace_onboarded_at, MINUTE) AS reached_test_pred_made_minutes,
        TIMESTAMP_DIFF(made_prod_pred_at, workspace_onboarded_at, MINUTE) AS reached_prod_pred_made_minutes,
        TIMESTAMP_DIFF(made_50_prod_pred_at, workspace_onboarded_at, MINUTE) AS reached_50_prod_pred_made_minutes
    FROM
        workspace_onboarded wo
    LEFT JOIN cloned_template USING(workspace_id, email)
    LEFT JOIN retrained_model USING(workspace_id, email)
    LEFT JOIN made_test_pred USING(workspace_id, email)
    LEFT JOIN made_prod_pred USING(workspace_id, email)
    LEFT JOIN made_50_prod_pred USING(workspace_id, email)
    WHERE
        NOT(wo.email LIKE "%@levity.ai")

), 

median_workaround AS (

    SELECT
        year_month,
        year,
        month, 
        ANY_VALUE(reached_template_cloned_minutes_mdn) AS reached_template_cloned_minutes_mdn,
        ANY_VALUE(reached_retrained_model_minutes_mdn) AS reached_retrained_model_minutes_mdn,
        ANY_VALUE(reached_test_pred_made_minutes_mdn) AS reached_test_pred_made_minutes_mdn,
        ANY_VALUE(reached_prod_pred_made_minutes_mdn) AS reached_prod_pred_made_minutes_mdn,
        ANY_VALUE(reached_50_prod_pred_made_minutes_mdn) AS reached_50_prod_pred_made_minutes_mdn
    FROM
    (SELECT
        year,
        month,
        year_month,
        PERCENTILE_CONT(reached_template_cloned_minutes, 0.5) OVER(PARTITION BY year_month) AS reached_template_cloned_minutes_mdn,
        PERCENTILE_CONT(reached_retrained_model_minutes, 0.5) OVER(PARTITION BY year_month) AS reached_retrained_model_minutes_mdn,
        PERCENTILE_CONT(reached_test_pred_made_minutes, 0.5) OVER(PARTITION BY year_month) AS reached_test_pred_made_minutes_mdn,
        PERCENTILE_CONT(reached_prod_pred_made_minutes, 0.5) OVER(PARTITION BY year_month) AS reached_prod_pred_made_minutes_mdn,
        PERCENTILE_CONT(reached_50_prod_pred_made_minutes, 0.5) OVER(PARTITION BY year_month) AS reached_50_prod_pred_made_minutes_mdn
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
    COUNT(CASE WHEN template_cloned_at IS NOT NULL THEN 1 END) AS reached_template_cloned_count,
    AVG(reached_template_cloned_minutes) AS reached_template_cloned_minutes_avg,
    ANY_VALUE(reached_template_cloned_minutes_mdn) AS reached_template_cloned_minutes_mdn,
    COUNT(CASE WHEN retrained_model_at IS NOT NULL THEN 1 END) AS reached_retrained_model_count,
    AVG(reached_retrained_model_minutes) AS reached_retrained_model_minutes_avg,
    ANY_VALUE(reached_retrained_model_minutes_mdn) AS reached_retrained_model_minutes_mdn,
    COUNT(CASE WHEN made_test_pred_at IS NOT NULL THEN 1 END) AS reached_test_pred_made_count,
    AVG(reached_test_pred_made_minutes) AS reached_test_pred_made_minutes_avg,
    ANY_VALUE(reached_test_pred_made_minutes_mdn) AS reached_test_pred_made_minutes_mdn,
    COUNT(CASE WHEN made_prod_pred_at IS NOT NULL THEN 1 END) AS reached_prod_pred_made_count,
    AVG(reached_prod_pred_made_minutes) AS reached_prod_pred_made_minutes_avg,
    ANY_VALUE(reached_prod_pred_made_minutes_mdn) AS reached_prod_pred_made_minutes_mdn,
    COUNT(CASE WHEN made_50_prod_pred_at IS NOT NULL THEN 1 END) AS reached_50_prod_pred_made_count,
    AVG(reached_50_prod_pred_made_minutes) AS reached_50_prod_pred_made_minutes_avg,
    ANY_VALUE(reached_50_prod_pred_made_minutes_mdn) AS reached_50_prod_pred_made_minutes_mdn
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





