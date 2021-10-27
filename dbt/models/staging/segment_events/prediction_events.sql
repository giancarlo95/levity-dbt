WITH prediction_models_classifier AS (

    SELECT 
        user_id,
        aiblock_id,
        classifier_id
    FROM 
        {{ref('prediction_models_classifier')}}

), onboarded_users AS (

    SELECT
        user_id,
        user_email_address
    FROM
        {{ref('onboarded_users')}}

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
        MAX(pmp.date_prediction_made) AS time_stamp
    FROM prediction_models_prediction pmp
    INNER JOIN prediction_models_classifier pmc ON pmp.classifier_id = pmc.classifier_id
    WHERE DATE(TIMESTAMP_TRUNC(pmp.date_prediction_made, DAY)) = DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY)
    GROUP BY 1, 2, 3
<<<<<<< HEAD
    ORDER BY 4 DESC
=======
    ORDER BY 3 DESC
>>>>>>> 70404971c39a5e1781675504ed0a4d962b05d253
)


SELECT
<<<<<<< HEAD
    final.user_id,
    company_id,
    aiblock_id,
    obu.user_email_address AS email_address,
    total_predictions_24h,
    time_stamp
FROM final
INNER JOIN onboarded_users obu ON final.user_id = obu.user_id
=======
    *
FROM 
    final
WHERE 
    DATE(TIMESTAMP_TRUNC(time_stamp, DAY)) = DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY)
>>>>>>> 70404971c39a5e1781675504ed0a4d962b05d253
