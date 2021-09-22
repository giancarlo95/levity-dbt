WITH prediction_models_prediction AS (

    SELECT 
        * 
    FROM {{ ref('prediction_models_prediction') }}

), onboarded_accounts AS (

    SELECT 
        * 
    FROM {{ ref('onboarded_accounts') }}

), workflows_classifierblock AS (

    SELECT 
        classifier_id,
        block_id
    FROM 
        {{ref('workflows_classifierblock')}}

), datasets_dataset AS (

    SELECT 
        aiblock_id,
        is_template
    FROM
        {{ref('datasets_dataset')}}

), prediction_models_classifier AS (

    SELECT 
        classifier_id,
        aiblock_id
    FROM 
        {{ref('prediction_models_classifier')}}

), workflows_flowstep AS (

    SELECT 
        DISTINCT flow_id, date_step_created
    FROM 
        {{ref('workflows_flowstep')}}

), workflows_block AS (

    SELECT 
        *
    FROM 
        {{ref('workflows_block')}}

), prediction_models_classifier_enriched AS (

    SELECT 
        classifier_id,
        prediction_models_classifier.aiblock_id,
        is_template
    FROM prediction_models_classifier
    INNER JOIN datasets_dataset ON 
        prediction_models_classifier.aiblock_id=datasets_dataset.aiblock_id  

), workflows_block_enriched AS (

    SELECT 
        flow_id,
        workflows_block.block_id,
        workflows_classifierblock.classifier_id,
        is_template
    FROM workflows_block
    INNER JOIN workflows_classifierblock ON 
        workflows_classifierblock.block_id=workflows_block.block_id
    INNER JOIN prediction_models_classifier_enriched ON
        prediction_models_classifier_enriched.classifier_id=workflows_classifierblock.classifier_id

) 

SELECT 
    prediction_models_prediction.account_id,
    prediction_models_prediction.date_prediction_made,
    workflows_block_enriched.is_template,
    workflows_flowstep.flow_id
FROM 
    prediction_models_prediction
INNER JOIN onboarded_accounts 
    ON onboarded_accounts.logged_account_id=prediction_models_prediction.account_id
INNER JOIN workflows_block_enriched ON
    workflows_block_enriched.classifier_id=prediction_models_prediction.classifier_id
INNER JOIN workflows_flowstep ON
    workflows_flowstep.flow_id=workflows_block_enriched.flow_id AND DATETIME_DIFF(prediction_models_prediction.date_prediction_made, workflows_flowstep.date_step_created, SECOND)<5 AND DATETIME_DIFF(prediction_models_prediction.date_prediction_made, workflows_flowstep.date_step_created, SECOND)>-5 
