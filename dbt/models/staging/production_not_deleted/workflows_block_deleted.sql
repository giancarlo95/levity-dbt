WITH workflows_block AS (

    SELECT
       * 
    FROM 
       {{ref('workflows_block')}}

), block_deleted AS (

    SELECT 
       *
    FROM 
       {{ref('block_deleted')}}

)

SELECT 
    workflows_block.block_id,			
    date_block_created,		
    old_user_id,
    user_id,
    account_id,	
    parent_id,	
    is_trigger,	
    block_type,		
    date_block_updated,		
    flow_id
FROM 
    workflows_block
LEFT JOIN 
    block_deleted ON workflows_block.block_id=block_deleted.block_id
WHERE
    block_deleted.block_id IS NULL