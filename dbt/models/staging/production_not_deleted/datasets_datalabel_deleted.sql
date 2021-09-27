WITH datasets_datalabel AS (

    SELECT * FROM {{ref('datasets_datalabel')}}

), label_deleted AS (

    SELECT * FROM {{ref('label_deleted')}}

), final AS (

    SELECT 
        id,					
        bounding_box_id,		
        classifier_version_id,			
        confidence_score,		
        date_labelled_datapoint_uploaded,			
        datapoint_id,			
        datasets_datalabel.label_id,			
        old_user_id,
        user_id,
        account_id,
        task_action_id,			
        date_labelled_datapoint_updated,
        _airbyte_emitted_at,	
        _airbyte_production_datasets_datalabel_hashid
    FROM 
        datasets_datalabel
    LEFT JOIN 
        label_deleted ON label_deleted.label_id=datasets_datalabel.label_id
    WHERE
        label_deleted.label_id IS NULL

), classifier_version_deleted AS (

    SELECT
       * 
    FROM 
       {{ref('classifier_version_deleted')}}
    
)

SELECT 
    id,					
    bounding_box_id,		
    final.classifier_version_id,			
    confidence_score,		
    date_labelled_datapoint_uploaded,			
    datapoint_id,			
    label_id,			
    old_user_id,
    user_id,
    account_id,
    task_action_id,			
    date_labelled_datapoint_updated,
    _airbyte_emitted_at,	
    _airbyte_production_datasets_datalabel_hashid
FROM 
    final
LEFT JOIN 
    classifier_version_deleted ON classifier_version_deleted.version_id=final.classifier_version_id
WHERE
    classifier_version_deleted.version_id IS NULL

 