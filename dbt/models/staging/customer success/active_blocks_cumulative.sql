WITH prediction_models_classifierversion_deleted AS (

    SELECT 
        classifier_id,
        date_version_created,
        account_id
    FROM 
        {{ref('prediction_models_classifierversion_deleted')}}

)

SELECT 
    classifier_id,
    account_id,
    MIN(date_version_created) AS first_aiblock_trained
FROM 
    prediction_models_classifierversion_deleted
GROUP BY
    classifier_id,
    account_id