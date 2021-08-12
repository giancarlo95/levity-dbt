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
        aiblock_description
    FROM
        {{ref('datasets_dataset_deleted')}}
    WHERE 
        aiblock_description IS NULL

), prediction_models_classifierversion AS (

    SELECT 
        version_id,
        classifier_id
    FROM 
        {{ref('prediction_models_classifierversion_deleted')}}

), prediction_models_classifier_filtered AS (

    SELECT 
        classifier_id
    FROM prediction_models_classifier
    INNER JOIN datasets_dataset ON 
        prediction_models_classifier.aiblock_id=datasets_dataset.aiblock_id  

), prediction_models_classifierversion_filtered AS (

    SELECT 
        version_id
    FROM prediction_models_classifierversion
    INNER JOIN prediction_models_classifier_filtered ON 
        prediction_models_classifierversion.classifier_id=prediction_models_classifier_filtered.classifier_id 

)

SELECT 
    user_id,
    MIN(date_training_run) AS date_first_training_run
FROM prediction_models_trainingrun
INNER JOIN prediction_models_classifierversion_filtered ON
    prediction_models_classifierversion_filtered.version_id=prediction_models_trainingrun.version_id
GROUP BY
    user_id
    



