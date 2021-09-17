WITH prediction_models_trainingrun AS (

    SELECT * FROM {{ref('prediction_models_trainingrun')}}

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

), prediction_models_classifierversion AS (

    SELECT 
        classifier_id,
        version_id
    FROM 
        {{ref('prediction_models_classifierversion_deleted')}}
    WHERE 
        performance_score>=0.9

), prediction_models_classifier_enriched AS (

    SELECT 
        classifier_id,
        is_template
    FROM prediction_models_classifier
    INNER JOIN datasets_dataset ON 
        prediction_models_classifier.aiblock_id=datasets_dataset.aiblock_id  

), prediction_models_classifierversion_enriched AS (

    SELECT 
        prediction_models_classifierversion.classifier_id,    
        version_id,
        is_template
    FROM prediction_models_classifierversion
    INNER JOIN prediction_models_classifier_enriched ON 
        prediction_models_classifierversion.classifier_id=prediction_models_classifier_enriched.classifier_id 

), onboarded_accounts AS (

    SELECT 
       *
    FROM 
       {{ref('onboarded_accounts')}}

)

SELECT 
    prediction_models_trainingrun.account_id,
    prediction_models_classifierversion_enriched.classifier_id,
    is_template,
    CASE 
        WHEN onboarded_accounts.customer_status="Paid Plan" THEN "Paid Plan"
        ELSE "Design Partner"
    END                                         AS customer_status_binary,
    MIN(date_training_run) AS date_first_training_run
FROM prediction_models_trainingrun
INNER JOIN prediction_models_classifierversion_enriched ON
    prediction_models_classifierversion_enriched.version_id=prediction_models_trainingrun.version_id
INNER JOIN onboarded_accounts ON
    onboarded_accounts.account_id=prediction_models_trainingrun.account_id
GROUP BY
    prediction_models_trainingrun.account_id,
    prediction_models_classifierversion_enriched.classifier_id,
    is_template,
    CASE 
        WHEN onboarded_accounts.customer_status="Paid Plan" THEN "Paid Plan"
        ELSE "Design Partner"
    END                                         
    



