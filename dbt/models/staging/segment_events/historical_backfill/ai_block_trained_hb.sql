WITH prediction_model_classifierversion AS (

    SELECT 
        *
    FROM 
        {{ref('prediction_model_classifierversion')}}

), prediction_model_classifier AS (
    
    SELECT 
        *
    FROM 
        {{ref('prediction_model_classifier')}}

), prediction_model_trainingrun AS (
    
    SELECT 
        *
    FROM 
        {{ref('prediction_model_trainingrun')}}
    WHERE version_id IS NOT NULL;

), model_performance (

    SELECT 
        performance_score, 
        aiblock_id,
        user_id,
        account_id, 
        classifier_id
    FROM prediction_model_classifierversion 
    INNER JOIN prediction_model_classifier pmc ON pmcv.classifier_id = pmc.classifier_id

), date_trained AS (

    SELECT
        date_version_created,
        classifier_id
    FROM prediction_model_classifier pmc
    INNER JOIN prediction_model_trainingrun pmt ON pmc.version_id = pmt.version_id
)

SELECT
    user_id,
    account_id,
    aiblock_id,
    performance_score,
    date_version_created
FROM model_performance mp 
INNER JOIN date_trained dt ON mp.classifier_id = dt.classifier_id



