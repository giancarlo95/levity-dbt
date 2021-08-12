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
        old_user_id,
        prediction_models_classifierversion.classifier_id,
        version_id,			
        date_version_created,
        date_version_updated,
        performance_score
    FROM prediction_models_classifierversion
    INNER JOIN prediction_models_classifier_filtered ON 
        prediction_models_classifierversion.classifier_id=prediction_models_classifier_filtered.classifier_id

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

), final AS (

    SELECT
        onboarded_users.user_email_address,
        prediction_models_classifierversion_filtered.classifier_id,
        prediction_models_classifierversion_filtered.version_id,
        performance_score,
        CASE 
            WHEN prediction_models_trainingrun.version_id IS NULL THEN 1 
            ELSE 0 
        END                                                                                                       AS is_deleted,
        CASE 
            WHEN onboarded_users.user_id IS NULL THEN 0 
            ELSE 1 
        END                                                                                                       AS is_approved,
        date_training_run,
        CASE 
            WHEN performance_score<0.9 THEN 0 
            WHEN performance_score IS NULL THEN NULL
            ELSE 1 
        END                                                                                                       AS is_good
    FROM prediction_models_classifierversion_filtered
    LEFT JOIN onboarded_users
         ON onboarded_users.user_id=prediction_models_classifierversion_filtered.user_id
    LEFT JOIN prediction_models_trainingrun
         ON prediction_models_trainingrun.version_id=prediction_models_classifierversion_filtered.version_id

), final_last AS (

    SELECT 
        user_email_address,
        classifier_id,
        MAX(date_training_run) AS last_training
    FROM 
        final
    WHERE 
        is_approved=1 
        AND is_deleted=0
    GROUP BY
        user_email_address,
        classifier_id

)

SELECT 
    final_last.user_email_address,
    final_last.classifier_id,
    last_training,
    performance_score
FROM final_last LEFT JOIN final ON 
    final.classifier_id=final_last.classifier_id 
    AND final.date_training_run=final_last.last_training
