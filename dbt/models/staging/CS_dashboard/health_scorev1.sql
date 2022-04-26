{# WITH prediction_models_classifier AS (

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
        origin,
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
        workspace_id
    FROM
        {{ref('workspaces')}}
        
), final AS (

    SELECT
        IFNULL(pmp.user_id, pmc.user_id)                      AS user_id,
        pmp.workspace_id,
        pmc.aiblock_id                                        AS aiblock_id,
        is_template,
        aiblock_name,
        aiblock_description,
        is_hitl,
        origin,
        workflow_id,
        TIMESTAMP_TRUNC(pmp.date_prediction_made, HOUR)       AS relevant_day_hour,
        COUNT(pmp.prediction_id)                              AS total_predictions,
        MAX(date_prediction_made)                             AS time_stamp
    FROM prediction_models_prediction pmp
    INNER JOIN prediction_models_classifier pmc ON pmp.classifier_id = pmc.classifier_id
    INNER JOIN datasets_dataset dd ON dd.aiblock_id=pmc.aiblock_id
    WHERE TIMESTAMP_TRUNC(pmp.date_prediction_made, HOUR) = TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP(),INTERVAL 2 HOUR), HOUR)
    GROUP BY 
        1, 
        2, 
        3, 
        4,
        5,
        6,
        7,
        8,
        9,
        10

)

SELECT
    user_id,
    final.workspace_id,
    aiblock_id,
    is_template,
    aiblock_name,
    aiblock_description,
    is_hitl,
    origin,
    workflow_id,
    total_predictions,
    time_stamp
FROM final
INNER JOIN workspaces oa ON final.workspace_id = oa.workspace_id #}


WITH engagement AS (

    SELECT 
        id,
        DATEDIFF(DAY, GETDATE(), properties_hs_last_sales_activity_timestamp_value) AS days_since_last_engagement,
        DATEDIFF(DAY, GETDATE(), properties_hs_email_last_reply_date_value) AS days_since_last_heard_from
        -- there's also the property 'last marketing email reply date' -> which one to use? or max of both?
    FROM 
        {{ref('hubspot_crm')}}

),

usage AS (

-- num actions
-- # user logged in last 7
-- # people in account (~)

),

adoption AS (

-- ai model trained
-- template used

),

