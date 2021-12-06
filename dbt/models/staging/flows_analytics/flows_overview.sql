WITH workflows_classifierblock AS (

    SELECT 
        classifier_id,
        block_id
    FROM 
        {{ref('workflows_classifierblock')}}

), workflows_block AS (

    SELECT 
        block_id,
        is_trigger,
        block_type,
        flow_id
    FROM 
        {{ref('workflows_block')}}

), workflows_blendrblock AS (

    SELECT 
        block_id,
        blendrdatasource_id,
        blendrtemplate_id
    FROM 
        {{ref('workflows_blendrblock')}}

), workflows_blendrtemplate AS (

    SELECT 
        blendrtemplate_id,
        blendrtemplate_name,
        blendrtemplate_description,
        blendrtemplate_type
    FROM
        {{ref('workflows_blendrtemplate')}}

), workflows_blendrdatasource AS (

    SELECT 
        blendrdatasource_id,
        blendrdatasource_name
    FROM
        {{ref('workflows_blendrdatasource')}}
    
)

SELECT 
    workflows_block.flow_id,
    workflows_block.block_id,
