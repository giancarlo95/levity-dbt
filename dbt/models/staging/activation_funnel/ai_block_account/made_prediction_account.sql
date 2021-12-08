WITH prediction_models_prediction AS (

    SELECT * FROM {{ref('prediction_models_prediction')}}

), prediction_models_classifier AS (

    SELECT 
        classifier_id,
        aiblock_id
    FROM 
        {{ref('prediction_models_classifier')}}

), datasets_dataset AS (

    SELECT 
        aiblock_id,
        aiblock_description
    FROM
        {{ref('datasets_dataset')}}
    WHERE 
        is_template="no"

), prediction_models_classifier_filtered AS (

    SELECT 
        classifier_id
    FROM prediction_models_classifier
    INNER JOIN datasets_dataset ON 
        prediction_models_classifier.aiblock_id=datasets_dataset.aiblock_id  

)

SELECT 
    account_id,
    MIN(date_prediction_made) AS date_first_prediction_made
FROM prediction_models_prediction
INNER JOIN prediction_models_classifier_filtered ON
    prediction_models_classifier_filtered.classifier_id=prediction_models_prediction.classifier_id
GROUP BY
    account_id
