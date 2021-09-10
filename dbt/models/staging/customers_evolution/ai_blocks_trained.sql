WITH prediction_models_trainingrun AS (

    SELECT 
    * 
    FROM {{ref('prediction_models_trainingrun')}}
    WHERE 
        version_id IS NOT NULL

), onboarded_users AS (

    SELECT 
        *
    FROM 
        {{ref('onboarded_users')}}

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

), prediction_models_classifierversion AS (

    SELECT 
        version_id,
        classifier_id,
        performance_score
    FROM 
        {{ref('prediction_models_classifierversion_deleted')}}

), prediction_models_classifier_filtered AS (

    SELECT 
        classifier_id,
        prediction_models_classifier.aiblock_id
    FROM prediction_models_classifier
    INNER JOIN datasets_dataset ON 
        prediction_models_classifier.aiblock_id=datasets_dataset.aiblock_id  

), prediction_models_classifierversion_filtered AS (

    SELECT 
        version_id,
        prediction_models_classifier_filtered.classifier_id,
        prediction_models_classifier_filtered.aiblock_id,
        performance_score
    FROM prediction_models_classifierversion
    INNER JOIN prediction_models_classifier_filtered ON 
        prediction_models_classifierversion.classifier_id=prediction_models_classifier_filtered.classifier_id 

)

SELECT 
    logged_account_id,
    aiblock_id,
    MIN(date_training_run) AS date_first_training_run,
    MAX(performance_score) AS performance,
    classifier_id
FROM prediction_models_trainingrun
INNER JOIN prediction_models_classifierversion_filtered ON
    prediction_models_classifierversion_filtered.version_id=prediction_models_trainingrun.version_id
INNER JOIN onboarded_users ON 
    prediction_models_trainingrun.user_id=onboarded_users.user_id
GROUP BY
    logged_account_id,
    aiblock_id,
    classifier_id
    



