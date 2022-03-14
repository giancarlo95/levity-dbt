WITH prediction_models_prediction AS (

    SELECT * FROM {{ ref('prediction_models_prediction') }}

), workspaces AS (

    SELECT * FROM {{ ref('workspaces') }}

)

SELECT 
    prediction_models_prediction.workspace_id,
    EXTRACT(DATE FROM date_prediction_made)            AS date,
    COUNT(*)                                           AS predictions_count
FROM 
    prediction_models_prediction
INNER JOIN 
    workspaces ON workspaces.workspace_id=prediction_models_prediction.workspace_id
GROUP BY
    prediction_models_prediction.workspace_id,
    EXTRACT(DATE FROM date_prediction_made)