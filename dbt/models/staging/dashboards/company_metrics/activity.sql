{{
  config(
    materialized = 'table',
    )
}}

WITH legacy_activity AS (

    SELECT
        *
    FROM
        {{ref("legacy_activity")}}
    
), users AS (

    SELECT 
        user_id
    FROM
        {{ref("users")}}
    
), app_predictions_done AS (

    SELECT 
        FORMAT_TIMESTAMP("%b %Y", timestamp) AS year_month,
        EXTRACT(YEAR FROM timestamp) AS year,
        EXTRACT(MONTH FROM timestamp) AS month,
        context_group_id AS workspace_id,
        COALESCE(origin, "unknown") AS origin,
        total_predictions
    FROM
        {{ref("django_production_predictions_done")}} pd
    INNER JOIN users USING(user_id)
    WHERE
        NOT(COALESCE(origin, "unknown") = "test_tab")
        AND DATE(timestamp)>="2022-04-01"

), vetevo_predictions_done AS (

    SELECT
        FORMAT_TIMESTAMP("%b %Y", date_prediction_made) AS year_month,
        EXTRACT(YEAR FROM date_prediction_made) AS year,
        EXTRACT(MONTH FROM date_prediction_made) AS month,
        SUM(predictions_count) AS vetevo_predictions_count
    FROM
        {{ref("vetevo")}}
    WHERE 
        date_prediction_made>="2022-04-01"
    GROUP BY
        1,
        2,
        3

), app_workspace_1 AS (

    SELECT
        year_month,
        year,
        month,
        COUNT(DISTINCT workspace_id) AS app_workspace_1_count,
        SUM(total_predictions) AS app_predictions_count
    FROM
        app_predictions_done
    GROUP BY
        1,
        2,
        3

), app_workspace_50 AS (

    SELECT
        year_month,
        year,
        month,
        COUNT(DISTINCT workspace_id) AS app_workspace_50_count
    FROM
    (SELECT
        year_month,
        year,
        month,
        workspace_id,
        SUM(total_predictions) AS monthly_predictions_count
    FROM
        app_predictions_done
    GROUP BY
        1,
        2,
        3,
        4
    HAVING
        SUM(total_predictions)>=50) AS grouped
    GROUP BY
        1,
        2,
        3

), app_workspace_50_flow AS (

    SELECT
        year_month,
        year,
        month,
        COUNT(DISTINCT workspace_id) AS app_workspace_50_flow_count
    FROM
    (SELECT
        year_month,
        year,
        month,
        workspace_id,
        SUM(total_predictions) AS monthly_predictions_count
    FROM
        app_predictions_done
    WHERE
        origin LIKE "flow%"
    GROUP BY
        1,
        2,
        3,
        4
    HAVING
        SUM(total_predictions)>=50) AS grouped
    GROUP BY
        1,
        2,
        3

), current_activity AS (

    SELECT 
        year_month,
        year,
        month,
        app_workspace_1_count AS at_least_1_prod_pred,
        app_workspace_50_count AS at_least_50_prod_pred,
        COALESCE(app_workspace_50_flow_count, 0) AS at_least_50_flow_pred,
        app_predictions_count,
        vetevo_predictions_count
    FROM
        vetevo_predictions_done
    LEFT JOIN app_workspace_1 USING(year_month, year, month)
    LEFT JOIN app_workspace_50 USING(year_month, year, month)
    LEFT JOIN app_workspace_50_flow USING(year_month, year, month)

)

SELECT * FROM legacy_activity UNION ALL 
SELECT * FROM current_activity
ORDER BY
    2 ASC,
    3 ASC
