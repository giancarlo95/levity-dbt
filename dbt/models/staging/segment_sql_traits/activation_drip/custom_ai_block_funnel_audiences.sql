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
        dppd.timestamp AS predictions_done_at,
        total_predictions
    FROM
        {{ref("django_production_predictions_done")}} dppd
    WHERE
        NOT(origin = "test_tab")
        AND is_template = "no"

),

predictions_1_done AS (

    SELECT 
        workspace_id,
        MIN(predictions_done_at) AS first_predictions_done_at
    FROM
        predictions_done pd
    GROUP BY
        workspace_id

),

predictions_50_done AS (

    SELECT
        workspace_id,
        MAX(predictions_done_at) AS last_predictions_done_at
    FROM 
        predictions_done
    GROUP BY
        1
    HAVING
        SUM(total_predictions)>=50

) 

SELECT
    *,
    CASE 
        WHEN last_predictions_done_at IS NOT NULL THEN "made_50+_predictions"
        WHEN first_predictions_done_at IS NOT NULL THEN "predictions_50_nudge"
        WHEN first_ai_block_trained_at IS NOT NULL THEN "predictions_1_nudge"
        WHEN first_datapoints_added_at IS NOT NULL THEN "ai_block_trained_nudge"
        WHEN first_ai_block_created_at IS NOT NULL THEN "datapoints_added_nudge"
        ELSE "ai_block_created_nudge"
    END AS custom_ai_block_funnel_activation_drip_audiences
FROM
    workspace_onboarded
LEFT JOIN ai_block_created USING(workspace_id)
LEFT JOIN datapoints_added USING(workspace_id)
LEFT JOIN ai_block_trained USING(workspace_id)
LEFT JOIN predictions_1_done USING(workspace_id)
LEFT JOIN predictions_50_done USING(workspace_id)
WHERE
    NOT(email LIKE "%@levity.ai")






