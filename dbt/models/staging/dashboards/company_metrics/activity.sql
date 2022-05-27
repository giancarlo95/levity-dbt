{{
  config(
    materialized = 'table',
    )
}}

WITH prediction_models_classifier AS (

    SELECT 
        user_id,
        aiblock_id,
        classifier_id
    FROM 
        {{ref('prediction_models_classifier')}}

), prediction_models_prediction AS (

    SELECT
        user_id,
        workspace_id,
        prediction_id,
        is_hitl,
        CASE 
            WHEN NOT(origin LIKE "flows%" OR origin LIKE "Integromat%" OR origin LIKE "Zapier%" OR origin = "test_tab" OR origin LIKE "Bubble%") THEN "API"
            ELSE origin
        END AS origin,
        workflow_id,
        date_prediction_made,
        classifier_id
    FROM 
        {{ref('prediction_models_prediction')}}

), datasets_dataset AS (

    SELECT
        user_id,
        workspace_id,
        aiblock_id,
        is_template,
        aiblock_name,
        aiblock_description
    FROM 
        {{ref('datasets_dataset')}}

), users AS (
    
    SELECT 
        *
    FROM
        {{ref('users')}}
 
), final_prod_all_sources AS (

    SELECT
        EXTRACT(MONTH FROM pmp.date_prediction_made)          AS month,
        EXTRACT(YEAR FROM pmp.date_prediction_made)           AS year,
        pmp.workspace_id,
        COUNT(pmp.prediction_id)                              AS predictions_count
    FROM prediction_models_prediction pmp
    INNER JOIN prediction_models_classifier pmc ON pmp.classifier_id = pmc.classifier_id
    INNER JOIN datasets_dataset dd ON dd.aiblock_id=pmc.aiblock_id
    INNER JOIN users u ON u.user_id=dd.user_id
    WHERE 
        NOT(origin = "test_tab")
        AND pmp.date_prediction_made >= "2022-01-01"
    GROUP BY 
        1, 
        2, 
        3

), final_prod_flows AS (

    SELECT
        EXTRACT(MONTH FROM pmp.date_prediction_made)          AS month,
        EXTRACT(YEAR FROM pmp.date_prediction_made)           AS year,
        pmp.workspace_id,
        COUNT(pmp.prediction_id)                              AS flow_predictions_count
    FROM prediction_models_prediction pmp
    INNER JOIN prediction_models_classifier pmc ON pmp.classifier_id = pmc.classifier_id
    INNER JOIN datasets_dataset dd ON dd.aiblock_id=pmc.aiblock_id
    INNER JOIN users u ON u.user_id=dd.user_id
    WHERE 
        origin LIKE "flows%"
        AND pmp.date_prediction_made >= "2022-01-01"
    GROUP BY 
        1, 
        2, 
        3

)

SELECT 
    CONCAT(CAST(year AS STRING), CAST(month AS STRING)) AS year_month,
    year,
    month,
    COALESCE(COUNT(DISTINCT a.workspace_id), 0) AS at_least_1_prod_pred_count,
    COALESCE(COUNT(CASE WHEN a.predictions_count>=50 THEN 1 END), 0) AS at_least_50_prod_pred_count,
    COALESCE(COUNT(CASE WHEN f.flow_predictions_count>=50 THEN 1 END), 0) AS at_least_50_flow_pred_count,
FROM
    final_prod_all_sources a
LEFT JOIN final_prod_flows f USING (year, month)
GROUP BY
    1,
    2,
    3
ORDER BY
    1 ASC