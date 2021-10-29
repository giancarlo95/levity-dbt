WITH prediction_models_trainingrun AS (

    SELECT 
        * 
    FROM 
        {{ref('prediction_models_trainingrun')}}
    WHERE 
        version_id IS NOT NULL

), prediction_models_classifier AS (

    SELECT 
        classifier_id,
        aiblock_id
    FROM 
        {{ref('prediction_models_classifier')}}

), prediction_models_classifierversion AS (

    SELECT 
        classifier_id,
        version_id,
        performance_score
    FROM 
        {{ref('prediction_models_classifierversion')}}

), prediction_models_classifierversion_enriched AS (

    SELECT 
        prediction_models_classifierversion.classifier_id,    
        aiblock_id,
        version_id,
        performance_score
    FROM prediction_models_classifierversion
    INNER JOIN prediction_models_classifier ON 
        prediction_models_classifierversion.classifier_id=prediction_models_classifier.classifier_id 

)

SELECT 
    prediction_models_trainingrun.account_id,
    prediction_models_trainingrun.user_id,
    aiblock_id,
    performance_score,
    date_training_run
FROM prediction_models_trainingrun
INNER JOIN prediction_models_classifierversion_enriched ON prediction_models_classifierversion_enriched.version_id=prediction_models_trainingrun.version_id
WHERE TIMESTAMP_DIFF(TIMESTAMP "2021-09-21 00:00:00+00", date_training_run, HOUR)>0


