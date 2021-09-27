WITH prediction_models_classifier AS (

    SELECT
       * 
    FROM 
       {{ref('prediction_models_classifier')}}

), classifier_deleted AS (

    SELECT 
       *
    FROM 
       {{ref('classifier_deleted')}}

)

SELECT 
    prediction_models_classifier.classifier_id,	
    created_at,		               
    aiblock_id,	
    emoji,		
    hitl_setting,		
    is_enabled,	
    max_error_rate,	
    old_user_id,
    user_id,
    account_id,		
    project_id,	
    status,		
    updated_at,	
    valohai_deployment_id,
    _airbyte_emitted_at,	
    _airbyte_production_prediction_models_classifier_hashid 
FROM 
    prediction_models_classifier
LEFT JOIN 
    classifier_deleted ON classifier_deleted.classifier_id=prediction_models_classifier.classifier_id
WHERE
    classifier_deleted.classifier_id IS NULL