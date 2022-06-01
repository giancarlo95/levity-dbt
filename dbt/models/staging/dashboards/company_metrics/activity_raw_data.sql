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
            WHEN NOT(origin LIKE "flows%" OR origin LIKE "Integromat%" OR origin LIKE "Zapier%" OR origin = "test_tab" OR origin LIKE "Bubble%") AND origin IS NOT NULL THEN "API"
            WHEN origin IS NULL THEN "unknown"
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

), workspaces AS (

    SELECT
        workspace_id,
        workspace_name
    FROM
        {{ref('workspaces')}}
        
), users AS (
    
    SELECT 
        *
    FROM
        {{ref('users')}}
 
), final AS (

    SELECT
        pmp.workspace_id,
        origin,
        EXTRACT(MONTH FROM pmp.date_prediction_made)          AS month,
        EXTRACT(YEAR FROM pmp.date_prediction_made)           AS year,
        COUNT(pmp.prediction_id)                              AS total_predictions
    FROM prediction_models_prediction pmp
    INNER JOIN prediction_models_classifier pmc ON pmp.classifier_id = pmc.classifier_id
    INNER JOIN datasets_dataset dd ON dd.aiblock_id=pmc.aiblock_id
    INNER JOIN users u ON u.user_id=dd.user_id
    WHERE 
        NOT(origin = "test_tab")
        AND pmp.date_prediction_made >= "2021-01-01"
    GROUP BY 
        1, 
        2, 
        3,
        4

)

SELECT
    CONCAT(CAST(year AS STRING), CAST(month AS STRING)) AS year_month,
    year,
    month,
    origin,
    workspace_name AS workspace,
    total_predictions
FROM final
INNER JOIN workspaces oa ON final.workspace_id = oa.workspace_id
ORDER BY
    year ASC,
    month ASC