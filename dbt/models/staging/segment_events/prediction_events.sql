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
        account_id,
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
        account_id,
        aiblock_id,
        is_template,
        aiblock_name,
        aiblock_description
    FROM 
        {{ref('datasets_dataset')}}

), onboarded_accounts AS (

    SELECT
        account_id,
        sample_user
    FROM
        {{ref('onboarded_accounts')}}
        
), final AS (

    SELECT
        IFNULL(pmp.user_id, pmc.user_id)                      AS user_id,
        pmp.account_id,
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
    final.account_id,
    sample_user,
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
INNER JOIN onboarded_accounts oa ON final.account_id = oa.account_id