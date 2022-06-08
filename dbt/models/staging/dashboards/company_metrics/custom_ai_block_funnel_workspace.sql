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

ai_block_created AS (

    SELECT 
        context_group_id AS workspace_id,
        MIN(abc.timestamp) AS first_ai_block_created_at
    FROM
        {{ref("django_production_ai_block_created")}} abc
    GROUP BY
        context_group_id

), 

datapoints_added AS (

    SELECT 
        context_group_id AS workspace_id,
        MIN(da.timestamp) AS first_datapoints_added_at
    FROM
        {{ref("django_production_datapoints_added")}} da
    WHERE
        is_human_in_the_loop = "no"
        AND is_template = "no"
    GROUP BY
        context_group_id

), 

ai_block_trained AS (

    SELECT 
        context_group_id AS workspace_id,
        MIN(abt.timestamp) AS first_ai_block_trained_at
    FROM
        {{ref("django_production_ai_block_trained")}} abt
    WHERE
        template = False
    GROUP BY
        context_group_id

), 

predictions_done AS (

    SELECT 
        context_group_id AS workspace_id,
        MIN(pd.timestamp) AS first_predictions_done_at
    FROM
        {{ref("django_production_predictions_done")}} pd
    WHERE
        NOT(origin = "test_tab")
        AND is_template = "no"
    GROUP BY
        context_group_id

), 

joined AS (

    SELECT
        EXTRACT(YEAR FROM workspace_onboarded_at) AS year,
        EXTRACT(MONTH FROM workspace_onboarded_at) AS month,
        FORMAT_TIMESTAMP("%b %Y", workspace_onboarded_at) AS year_month,
        *,
        TIMESTAMP_DIFF(first_ai_block_created_at, workspace_onboarded_at, MINUTE) AS reached_ai_block_created_minutes,
        TIMESTAMP_DIFF(first_datapoints_added_at, workspace_onboarded_at, MINUTE) AS reached_datapoints_added_minutes,
        TIMESTAMP_DIFF(first_ai_block_trained_at, workspace_onboarded_at, MINUTE) AS reached_ai_block_trained_minutes,
        TIMESTAMP_DIFF(first_predictions_done_at, workspace_onboarded_at, MINUTE) AS reached_predictions_done_minutes
    FROM
        workspace_onboarded
    LEFT JOIN ai_block_created USING(workspace_id)
    LEFT JOIN datapoints_added USING(workspace_id)
    LEFT JOIN ai_block_trained USING(workspace_id)
    LEFT JOIN predictions_done USING(workspace_id)
    WHERE
        NOT(email LIKE "%@levity.ai")

), 

median_workaround AS (

    SELECT
        year_month,
        year,
        month, 
        ANY_VALUE(reached_ai_block_created_minutes_mdn) AS reached_ai_block_created_minutes_mdn,
        ANY_VALUE(reached_datapoints_added_minutes_mdn) AS reached_datapoints_added_minutes_mdn,
        ANY_VALUE(reached_ai_block_trained_minutes_mdn) AS reached_ai_block_trained_minutes_mdn,
        ANY_VALUE(reached_predictions_done_minutes_mdn) AS reached_predictions_done_minutes_mdn
    FROM
    (SELECT
        year,
        month,
        year_month,
        PERCENTILE_CONT(reached_ai_block_created_minutes, 0.5) OVER(PARTITION BY year_month) AS reached_ai_block_created_minutes_mdn,
        PERCENTILE_CONT(reached_datapoints_added_minutes, 0.5) OVER(PARTITION BY year_month) AS reached_datapoints_added_minutes_mdn,
        PERCENTILE_CONT(reached_ai_block_trained_minutes, 0.5) OVER(PARTITION BY year_month) AS reached_ai_block_trained_minutes_mdn,
        PERCENTILE_CONT(reached_predictions_done_minutes, 0.5) OVER(PARTITION BY year_month) AS reached_predictions_done_minutes_mdn
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
    COUNT(CASE WHEN first_ai_block_created_at IS NOT NULL THEN 1 END) AS reached_ai_block_created_count,
    AVG(reached_ai_block_created_minutes) AS reached_ai_block_created_minutes_avg,
    ANY_VALUE(reached_ai_block_created_minutes_mdn) AS reached_ai_block_created_minutes_mdn,
    COUNT(CASE WHEN first_datapoints_added_at IS NOT NULL THEN 1 END) AS reached_datapoints_added_count,
    AVG(reached_datapoints_added_minutes) AS reached_datapoints_added_minutes_avg,
    ANY_VALUE(reached_datapoints_added_minutes_mdn) AS reached_datapoints_added_minutes_mdn,
    COUNT(CASE WHEN first_ai_block_trained_at IS NOT NULL THEN 1 END) AS reached_ai_block_trained_count,
    AVG(reached_ai_block_trained_minutes) AS reached_ai_block_trained_minutes_avg,
    ANY_VALUE(reached_ai_block_trained_minutes_mdn) AS reached_ai_block_trained_minutes_mdn,
    COUNT(CASE WHEN first_predictions_done_at IS NOT NULL THEN 1 END) AS reached_predictions_done_count,
    AVG(reached_predictions_done_minutes) AS reached_predictions_done_minutes_avg,
    ANY_VALUE(reached_predictions_done_minutes_mdn) AS reached_predictions_done_minutes_mdn
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





