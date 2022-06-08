{{
  config(
    materialized = 'table',
    )
}}

WITH users AS (

    SELECT 
        user_id,
        user_email_address
    FROM
        {{ref("users")}} u

),

user_onboarded AS (

    SELECT 
        user_id,
        MIN(DATE(uo.timestamp)) AS user_onboarded_at
    FROM
        {{ref("django_production_user_onboarded")}} uo
    GROUP BY
        user_id

), 

ai_block_created AS (

    SELECT 
        user_id,
        MIN(DATE(abc.timestamp)) AS first_ai_block_created_at
    FROM
        {{ref("django_production_ai_block_created")}} abc
    GROUP BY
        user_id

), 

datapoints_added AS (

    SELECT 
        user_id,
        MIN(DATE(da.timestamp)) AS first_datapoints_added_at
    FROM
        {{ref("django_production_datapoints_added")}} da
    WHERE
        is_human_in_the_loop = "no"
        AND is_template = "no"
    GROUP BY
        user_id

), 

ai_block_trained AS (

    SELECT 
        user_id,
        MIN(DATE(abt.timestamp)) AS first_ai_block_trained_at
    FROM
        {{ref("django_production_ai_block_trained")}} abt
    WHERE
        template = False
    GROUP BY
        user_id

), 

predictions_done AS (

    SELECT 
        user_id,
        MIN(DATE(pd.timestamp)) AS first_predictions_done_at
    FROM
        {{ref("django_production_predictions_done")}} pd
    WHERE
        NOT(origin = "test_tab")
        AND is_template = "no"
    GROUP BY
        user_id

), 

joined AS (

    SELECT
        * EXCEPT(user_email_address),
        user_email_address AS email
    FROM
        user_onboarded
    LEFT JOIN ai_block_created USING(user_id)
    LEFT JOIN datapoints_added USING(user_id)
    LEFT JOIN ai_block_trained USING(user_id)
    LEFT JOIN predictions_done USING(user_id)
    LEFT JOIN users USING(user_id)
    WHERE
        NOT(user_email_address LIKE "%@levity.ai")

), final AS (

    SELECT 
        EXTRACT(YEAR FROM user_onboarded_at) AS year,
        EXTRACT(MONTH FROM user_onboarded_at) AS month,
        COUNT(*) AS onboarded_count,
        COUNT(CASE WHEN first_ai_block_created_at IS NOT NULL THEN 1 END) AS reached_ai_block_created_count,
        COUNT(CASE WHEN first_datapoints_added_at IS NOT NULL THEN 1 END) AS reached_datapoints_added_count,
        COUNT(CASE WHEN first_ai_block_trained_at IS NOT NULL THEN 1 END) AS reached_ai_block_trained_count,
        COUNT(CASE WHEN first_predictions_done_at IS NOT NULL THEN 1 END) AS reached_predictions_done_count,
    FROM    
        joined
    GROUP BY
        1,
        2

)

SELECT
    FORMAT_DATE("%b %Y", PARSE_DATE("%Y%m", CONCAT(CAST(year AS STRING), CAST(month AS STRING)))) AS year_month,
    *
FROM
    final
ORDER BY
    1 ASC


