WITH prediction_models_prediction_large AS (

    SELECT
        classifier_id
	FROM
		{{ref('prediction_models_prediction')}}
	GROUP BY
        classifier_id
	HAVING
		COUNT(prediction_id)>=50

), prediction_models_prediction AS (

    SELECT 
        account_id,
        classifier_id,
        date_prediction_made
    FROM 
        {{ref('prediction_models_prediction')}}

), prediction_models_classifier AS (

    SELECT 
        classifier_id,
        aiblock_id
    FROM 
        {{ref('prediction_models_classifier_deleted')}}

), datasets_dataset AS (

    SELECT 
        aiblock_id,
        aiblock_description
    FROM
        {{ref('datasets_dataset_deleted')}}
    WHERE 
        aiblock_description IS NULL

), prediction_models_classifier_filtered AS (

    SELECT 
        classifier_id
    FROM prediction_models_classifier
    INNER JOIN datasets_dataset ON 
        prediction_models_classifier.aiblock_id=datasets_dataset.aiblock_id  

), final AS (

    SELECT 
        account_id,
        prediction_models_prediction_large.classifier_id,
        date_prediction_made
    FROM prediction_models_prediction
    INNER JOIN prediction_models_classifier_filtered ON 
        prediction_models_classifier_filtered.classifier_id=prediction_models_prediction.classifier_id
    INNER JOIN prediction_models_prediction_large ON
        prediction_models_prediction_large.classifier_id=prediction_models_prediction.classifier_id

), final_ordered AS (

    SELECT 
        account_id,
        classifier_id,
        date_prediction_made,
        ROW_NUMBER() OVER (PARTITION BY account_id, classifier_id ORDER BY date_prediction_made) AS RowNumber
    FROM 
        final

)

SELECT 
    account_id,
    MIN(date_prediction_made)     AS date_first_50predictions_made
FROM 
    final_ordered
WHERE 
    RowNumber=50
GROUP BY
    account_id




