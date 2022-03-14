WITH workspaces AS (

    SELECT 
        workspace_id             AS workspace_id,
        workspace_created_at     AS timing,
        'A.Onboarded workspaces' AS Funnel_step 
    FROM {{ref('workspaces')}}
       
), created_ai_block AS (

    SELECT 
        workspace_id,
        date_first_aiblock_created AS timing,
        'B.Created at least 1 AI Block' AS Funnel_step
    FROM {{ref('created_ai_block_workspace')}}

), uploaded_data AS (

    SELECT 
        workspace_id,
        date_first_somedata_uploaded AS timing,
        'C.Uploaded at least 1 Data point to at least 1 AI Block' AS Funnel_step
    FROM {{ref('uploaded_data_workspace')}}

), uploaded_40datapoints AS (

    SELECT
        workspace_id,
        date_first_40datapoints_uploaded AS timing,
        'D.Uploaded at least 40 Data points to at least 1 AI Block' AS Funnel_step
    FROM {{ref('uploaded_40datapoints_workspace')}}

), trained_ai_block AS (

    SELECT 
        workspace_id,
        date_first_training_run AS timing,
        'E.Trained at least 1 AI Block' AS Funnel_step
    FROM {{ref('trained_ai_block_workspace')}}

), made_prediction AS (

    SELECT 
        workspace_id,
        date_first_prediction_made AS timing,
        'F.Made at least 1 Prediction through at least 1 AI Block' AS Funnel_step
    FROM {{ref('made_prediction_workspace')}}

), made_50predictions AS (

    SELECT 
        workspace_id,
        date_first_50predictions_made AS timing,
        'G.Made at least 50 Predictions through at least 1 AI Block' AS Funnel_step
    FROM {{ref('made_50predictions_workspace')}}

)

SELECT * FROM workspaces UNION ALL
SELECT * FROM created_ai_block UNION ALL
SELECT * FROM uploaded_data UNION ALL
SELECT * FROM uploaded_40datapoints UNION ALL
SELECT * FROM trained_ai_block UNION ALL
SELECT * FROM made_prediction UNION ALL
SELECT * FROM made_50predictions
    

