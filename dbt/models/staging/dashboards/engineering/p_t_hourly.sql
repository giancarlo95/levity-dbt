{{
  config(
    materialized = 'table',
    )
}}

WITH pm_prediction AS (

    SELECT 
        *,
        EXTRACT(HOUR FROM created_at) AS hour
    FROM 
        {{ref('normalized_pm_prediction')}}
    WHERE
        op = "INSERT"

), pm_classifierversion AS (

    SELECT
        *,
        EXTRACT(HOUR FROM created_at) AS hour
    FROM  
        {{ref('normalized_pm_classifierversion')}}
    WHERE
        op="INSERT"

), pred_aggr AS (

    SELECT
        hour,
        COUNT(*)/7 AS pred_count
    FROM
        pm_prediction
    WHERE
        TIMESTAMP_DIFF(CURRENT_TIMESTAMP(),created_at, DAY)<=7
    GROUP BY
        hour

), train_aggr AS (

    SELECT
        hour,
        COUNT(*)/7 AS train_count
    FROM
        pm_classifierversion
    WHERE
        TIMESTAMP_DIFF(CURRENT_TIMESTAMP(),created_at, DAY)<=7
    GROUP BY
        hour

)

SELECT
    hour,
    COALESCE(pred_count,0) AS pred_count,
    COALESCE(train_count,0) AS train_count
FROM
    pred_aggr 
LEFT JOIN train_aggr USING(hour)
ORDER BY 
    hour ASC