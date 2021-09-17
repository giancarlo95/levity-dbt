WITH prediction_models_prediction AS (

    SELECT * FROM {{ref('prediction_models_prediction')}}

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
        prediction_models_classifier.aiblock_id,
        classifier_id,
        is_template
    FROM prediction_models_classifier
    INNER JOIN datasets_dataset ON 
        prediction_models_classifier.aiblock_id=datasets_dataset.aiblock_id  

), onboarded_accounts AS (

    SELECT 
       *
    FROM 
       {{ref('onboarded_accounts')}}

)

SELECT 
    prediction_models_prediction.account_id,
    aiblock_id,
    is_template,
    CASE 
        WHEN onboarded_accounts.customer_status="Paid Plan" THEN "Paid Plan"
        ELSE "Design Partner"
    END                          AS customer_status_binary,
    MIN(date_prediction_made)    AS date_first_prediction_made
FROM prediction_models_prediction
INNER JOIN prediction_models_classifier_enriched ON
    prediction_models_classifier_enriched.classifier_id=prediction_models_prediction.classifier_id
INNER JOIN onboarded_accounts ON
    onboarded_accounts.account_id=prediction_models_prediction.account_id
GROUP BY
    prediction_models_prediction.account_id,
    aiblock_id,
    is_template,
    CASE 
        WHEN onboarded_accounts.customer_status="Paid Plan" THEN "Paid Plan"
        ELSE "Design Partner"
    END                        
