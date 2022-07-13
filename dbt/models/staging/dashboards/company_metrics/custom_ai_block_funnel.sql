{{
  config(
    materialized = 'table',
    )
}}

WITH workspace_onboarded AS (

    SELECT 
        context_group_id AS workspace_id,
        email,
        MIN(uo.timestamp) AS onboarded_at,
        MIN(uo.timestamp) AS onboarded_at_string,
    FROM
        {{ref("django_production_user_onboarded")}} uo
    GROUP BY
        context_group_id,
        email

), 

created_ai_block AS (

    SELECT 
        *
    FROM
        {{ref("a_created_ai_block_workspace")}}

), 

data_added AS (

    SELECT 
        *
    FROM
        {{ref("b_uploaded_data_workspace")}} 

), 

added_40dp AS (

    SELECT 
        *
    FROM
        {{ref("c_uploaded_40datapoints_workspace")}} 

), 

trained_ai_block AS (

    SELECT 
        *
    FROM
        {{ref("d_trained_ai_block_workspace")}} 

), 

made_test_pred AS (

    SELECT 
        *
    FROM
        {{ref("e_made_test_pred_workspace")}} pd

), 

made_prod_pred AS (

    SELECT 
        *
    FROM
        {{ref("f_made_prod_pred_workspace")}} pd

), 

made_50_prod_pred AS (

    SELECT 
        *
    FROM
        {{ref("g_made_50_prod_pred_workspace")}} pd

),

joined AS (

    SELECT
        EXTRACT(YEAR FROM onboarded_at) AS year,
        EXTRACT(MONTH FROM onboarded_at) AS month,
        FORMAT_TIMESTAMP("%b %Y", onboarded_at) AS year_month,
        *,
        TIMESTAMP_DIFF(created_ai_block_at, onboarded_at, MINUTE) AS reached_created_ai_block_minutes,
        TIMESTAMP_DIFF(data_added_at, onboarded_at, MINUTE) AS reached_data_added_minutes,
        TIMESTAMP_DIFF(added_40dp_at, onboarded_at, MINUTE) AS reached_40dp_added_minutes,
        TIMESTAMP_DIFF(trained_ai_block_at, onboarded_at, MINUTE) AS reached_trained_ai_block_minutes,
        TIMESTAMP_DIFF(made_test_pred_at, onboarded_at, MINUTE) AS reached_made_test_pred_minutes,
        TIMESTAMP_DIFF(made_prod_pred_at, onboarded_at, MINUTE) AS reached_made_prod_pred_minutes,
        TIMESTAMP_DIFF(made_50_prod_pred_at, onboarded_at, MINUTE) AS reached_made_50_prod_pred_minutes
    FROM
        workspace_onboarded wo
    LEFT JOIN created_ai_block USING(workspace_id, email)
    LEFT JOIN data_added USING(workspace_id, email)
    LEFT JOIN added_40dp USING(workspace_id, email)
    LEFT JOIN trained_ai_block USING(workspace_id, email)
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
        ANY_VALUE(reached_created_ai_block_minutes_mdn) AS reached_created_ai_block_minutes_mdn,
        ANY_VALUE(reached_data_added_minutes_mdn) AS reached_data_added_minutes_mdn,
        ANY_VALUE(reached_40dp_added_minutes_mdn) AS reached_40dp_added_minutes_mdn,
        ANY_VALUE(reached_trained_ai_block_minutes_mdn) AS reached_trained_ai_block_minutes_mdn,
        ANY_VALUE(reached_made_test_pred_minutes_mdn) AS reached_made_test_pred_minutes_mdn,
        ANY_VALUE(reached_made_prod_pred_minutes_mdn) AS reached_made_prod_pred_minutes_mdn,
        ANY_VALUE(reached_made_50_prod_pred_minutes_mdn) AS reached_made_50_prod_pred_minutes_mdn
    FROM
    (SELECT
        year,
        month,
        year_month,
        PERCENTILE_CONT(reached_created_ai_block_minutes, 0.5) OVER(PARTITION BY year_month) AS reached_created_ai_block_minutes_mdn,
        PERCENTILE_CONT(reached_data_added_minutes, 0.5) OVER(PARTITION BY year_month) AS reached_data_added_minutes_mdn,
        PERCENTILE_CONT(reached_40dp_added_minutes, 0.5) OVER(PARTITION BY year_month) AS reached_40dp_added_minutes_mdn,
        PERCENTILE_CONT(reached_trained_ai_block_minutes, 0.5) OVER(PARTITION BY year_month) AS reached_trained_ai_block_minutes_mdn,
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
    COUNT(CASE WHEN created_ai_block_at IS NOT NULL THEN 1 END) AS reached_created_ai_block_count,
    AVG(reached_created_ai_block_minutes) AS reached_created_ai_block_minutes_avg,
    ANY_VALUE(reached_created_ai_block_minutes_mdn) AS reached_created_ai_block_minutes_mdn,
    COUNT(CASE WHEN data_added_at IS NOT NULL THEN 1 END) AS reached_data_added_count,
    AVG(reached_data_added_minutes) AS reached_data_added_minutes_avg,
    ANY_VALUE(reached_data_added_minutes_mdn) AS reached_data_added_minutes_mdn,
    COUNT(CASE WHEN added_40dp_at IS NOT NULL THEN 1 END) AS reached_40dp_added_count,
    AVG(reached_40dp_added_minutes) AS reached_40dp_added_minutes_avg,
    ANY_VALUE(reached_40dp_added_minutes_mdn) AS reached_40dp_added_minutes_mdn,
    COUNT(CASE WHEN trained_ai_block_at IS NOT NULL THEN 1 END) AS reached_trained_ai_block_count,
    AVG(reached_trained_ai_block_minutes) AS reached_trained_ai_block_minutes_avg,
    ANY_VALUE(reached_trained_ai_block_minutes_mdn) AS reached_trained_ai_block_minutes_mdn,
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





