WITH workflows_block AS (

    SELECT 
        * 
    FROM {{ref('workflows_block')}}
    WHERE 
        is_trigger=FALSE 
        AND block_type='classifier'

), workflows_classifierblock AS (

    SELECT 
        classifier_id,
        block_id
    FROM 
        {{ref('workflows_classifierblock')}}

), datasets_dataset AS (

    SELECT 
        aiblock_id,
        aiblock_description
    FROM
        {{ref('datasets_dataset')}}
    WHERE 
        aiblock_description IS NULL

), prediction_models_classifier AS (

    SELECT 
        classifier_id,
        aiblock_id
    FROM 
        {{ref('prediction_models_classifier')}}

), prediction_models_classifier_filtered AS (

    SELECT 
        classifier_id
    FROM prediction_models_classifier
    INNER JOIN datasets_dataset ON 
        prediction_models_classifier.aiblock_id=datasets_dataset.aiblock_id  

), workflows_classifierblock_filtered AS (

    SELECT 
        block_id
    FROM workflows_classifierblock
    INNER JOIN prediction_models_classifier_filtered ON 
        workflows_classifierblock.classifier_id=prediction_models_classifier_filtered.classifier_id 

)

SELECT 
    user_id,
    MIN(date_block_created) AS date_first_aiblock_connected
FROM workflows_block
INNER JOIN workflows_classifierblock_filtered ON
    workflows_classifierblock_filtered.block_id=workflows_block.block_id
GROUP BY
    user_id
    



