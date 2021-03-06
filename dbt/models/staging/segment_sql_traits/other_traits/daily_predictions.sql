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
        TIMESTAMP_TRUNC(pmp.date_prediction_made, DAY)        AS relevant_day,
        COUNT(pmp.prediction_id)                              AS total_predictions,
        MAX(date_prediction_made)                             AS time_stamp
    FROM prediction_models_prediction pmp
    INNER JOIN prediction_models_classifier pmc ON pmp.classifier_id = pmc.classifier_id
    INNER JOIN datasets_dataset dd ON dd.aiblock_id=pmc.aiblock_id
    INNER JOIN users u ON u.user_id=dd.user_id
    WHERE TIMESTAMP_TRUNC(pmp.date_prediction_made, DAY) =  TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY), DAY)
    GROUP BY 
        1, 
        2, 
        3

)

SELECT
    final.workspace_id,
    workspace_name AS workspace,
    origin,
    total_predictions,
    time_stamp
FROM final
INNER JOIN workspaces oa ON final.workspace_id = oa.workspace_id