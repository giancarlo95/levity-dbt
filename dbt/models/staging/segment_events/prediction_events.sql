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
        date_prediction_made,
        classifier_id
    FROM 
        {{ref('prediction_models_prediction')}}

), datasets_dataset AS (

    SELECT
        user_id,
        account_id,
        aiblock_id,
        is_template
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
        DATE_TRUNC(pmp.date_prediction_made, DAY)             AS relevant_day,
        COUNT(pmp.prediction_id)                              AS total_predictions,
        MAX(date_prediction_made)                             AS time_stamp
    FROM prediction_models_prediction pmp
    INNER JOIN prediction_models_classifier pmc ON pmp.classifier_id = pmc.classifier_id
    INNER JOIN datasets_dataset dd ON dd.aiblock_id=pmc.aiblock_id
    WHERE DATE(TIMESTAMP_TRUNC(pmp.date_prediction_made, DAY)) = DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY)
    GROUP BY 
        1, 
        2, 
        3, 
        4,
        5

)

SELECT
    user_id,
    final.account_id,
    sample_user,
    aiblock_id,
    is_template,
    total_predictions,
    time_stamp
FROM final
INNER JOIN onboarded_accounts oa ON final.account_id = oa.account_id