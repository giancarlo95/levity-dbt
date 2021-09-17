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
        is_template
    FROM
        {{ref('datasets_dataset_deleted')}}

), prediction_models_classifier_enriched AS (

    SELECT 
        classifier_id,
        prediction_models_classifier.aiblock_id,
        is_template
    FROM prediction_models_classifier
    INNER JOIN datasets_dataset ON 
        prediction_models_classifier.aiblock_id=datasets_dataset.aiblock_id  

), onboarded_accounts AS (

    SELECT 
       *
    FROM 
       {{ref('onboarded_accounts')}}

), final AS (

    SELECT 
        prediction_models_prediction.account_id,
        prediction_models_classifier_enriched.aiblock_id,
        is_template,
        CASE 
            WHEN onboarded_accounts.customer_status="Paid Plan" THEN "Paid Plan"
            ELSE "Design Partner"
        END                                         AS customer_status_binary,
        date_prediction_made
    FROM prediction_models_prediction
    INNER JOIN prediction_models_classifier_enriched ON 
        prediction_models_classifier_enriched.classifier_id=prediction_models_prediction.classifier_id
    INNER JOIN prediction_models_prediction_large ON
        prediction_models_prediction_large.classifier_id=prediction_models_prediction.classifier_id
    INNER JOIN onboarded_accounts ON
        onboarded_accounts.account_id=prediction_models_prediction.account_id

), final_ordered AS (

    SELECT 
        account_id,
        aiblock_id,
        is_template,
        customer_status_binary,
        date_prediction_made,
        ROW_NUMBER() OVER (PARTITION BY account_id, aiblock_id ORDER BY date_prediction_made) AS RowNumber
    FROM 
        final

)

SELECT 
    *
FROM 
    final_ordered
WHERE 
    RowNumber=50




