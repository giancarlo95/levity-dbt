{{
  config(
    materialized = 'table',
    )
}}

WITH prediction_models_classifier AS (

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
        aiblock_description IS NULL

), prediction_models_classifierversion AS (

    SELECT 
        *
    FROM 
        {{ref('prediction_models_classifierversion')}}

), prediction_models_classifier_filtered AS (

    SELECT 
        classifier_id
    FROM prediction_models_classifier
    INNER JOIN datasets_dataset ON 
        prediction_models_classifier.aiblock_id=datasets_dataset.aiblock_id  

), prediction_models_classifierversion_filtered AS (

    SELECT 
        user_id,
        prediction_models_classifierversion.classifier_id,
        version_id,			
        date_version_created,
        date_version_updated,
        performance_score
    FROM prediction_models_classifierversion
    INNER JOIN prediction_models_classifier_filtered ON 
        prediction_models_classifierversion.classifier_id=prediction_models_classifier_filtered.classifier_id 

), datasets_data AS (

    SELECT 
        user_id,
        aiblock_id, 
        datapoint_id,
        date_datapoint_uploaded,
        date_datapoint_updated
    FROM     
        {{ref('datasets_data')}}
    
), datasets_data_enriched AS (

    SELECT 
        datasets_data.user_id,
        datasets_data.aiblock_id,
        prediction_models_classifier.classifier_id,
        datasets_data.datapoint_id,
        datasets_data.date_datapoint_uploaded,
        datasets_data.date_datapoint_updated
    FROM datasets_data 
    LEFT JOIN prediction_models_classifier
        ON datasets_data.aiblock_id=prediction_models_classifier.aiblock_id

), onboarded_users AS (

    SELECT 
        * 
    FROM 
        {{ref('onboarded_users')}}

), prediction_models_trainingrun AS (

    SELECT 
        *
    FROM 
        {{ref('prediction_models_trainingrun')}}

), intercom__contact_enhanced AS (

    SELECT 
        *
    FROM 
        {{ref('intercom__contact_enhanced')}}

)

SELECT 
    prediction_models_classifierversion_filtered.user_id,
    user_email_address,
    custom_type_of_data,
    custom_use_case_ai_block,
    prediction_models_classifierversion_filtered.classifier_id,
    prediction_models_classifierversion_filtered.version_id,
    CASE WHEN prediction_models_trainingrun.version_id IS NULL THEN 1 ELSE 0 END AS is_deleted, 		
    --date_version_created,
    --date_version_updated,
    date_training_run,
    --date_training_updated,
    performance_score
    --SUM(sample_size) incremental_sample_size
FROM prediction_models_classifierversion_filtered
INNER JOIN onboarded_users
     ON onboarded_users.user_id=prediction_models_classifierversion_filtered.user_id
LEFT JOIN prediction_models_trainingrun
     ON prediction_models_trainingrun.version_id=prediction_models_classifierversion_filtered.version_id
LEFT JOIN intercom__contact_enhanced
    ON intercom__contact_enhanced.contact_email=onboarded_users.user_email_address
--LEFT JOIN (
--    SELECT 
--        classifier_id,
--        date_datapoint_updated,
--        COUNT(datapoint_id) AS sample_size
--    FROM
--        datasets_data_enriched
--    GROUP BY 
--        classifier_id,
--        date_datapoint_updated
--) AS interm ON prediction_models_classifierversion_filtered.classifier_id=interm.classifier_id AND date_version_created>=date_datapoint_updated
--GROUP BY 
--    user_id,
--    prediction_models_classifierversion_filtered.classifier_id,
--    version_id,
--    performance_score,
--    date_version_created


