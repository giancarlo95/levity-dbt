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

), final AS (

    SELECT
        IFNULL(pmp.user_id, pmc.user_id) AS user_id,
        pmp.account_id AS company_id,
        pmc.aiblock_id AS aiblock_id,
        COUNT(pmp.prediction_id) AS total_predictions_24h,
        DATE_TRUNC(pmp.date_prediction_made, DAY) AS time_stamp 
    FROM prediction_models_prediction pmp
    INNER JOIN prediction_models_classifier pmc ON pmp.classifier_id = pmc.classifier_id
    GROUP BY 1, 2, 3, 5
    ORDER BY 4 DESC
)


SELECT
    *
FROM final
