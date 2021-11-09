WITH prediction_models_classifierversion AS (

    SELECT
       * 
    FROM 
       {{ref('prediction_models_classifierversion')}}

), classifier_version_deleted AS (

    SELECT 
       *
    FROM 
       {{ref('classifier_version_deleted')}}

)

SELECT 
    prediction_models_classifierversion.version_id,			
    classifier_id,		
    date_version_created,		
    --default,	
    evaluation,		
    fake,		
    old_user_id,
    user_id,
    account_id,		
    performance_score,		
    seconds_left,		
    status,	
    template,		
    training_progress,		
    date_version_updated,		
    valohai_endpoint_id,	
    valohai_endpoint_url,	
    valohai_execution_id,	
    valohai_version_id,	
    valohai_version_name
FROM 
    prediction_models_classifierversion
LEFT JOIN 
    classifier_version_deleted ON classifier_version_deleted.version_id=prediction_models_classifierversion.version_id
WHERE
    classifier_version_deleted.version_id IS NULL