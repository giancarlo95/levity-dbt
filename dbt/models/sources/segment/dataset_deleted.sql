WITH source AS (

    SELECT * FROM {{ source('django_backend_api', 'dataset_deleted') }}

),

renamed AS (

    SELECT
        --context_library_name			
        --context_library_version			
        event                                  AS event_code,			
        --event_text			
        --id			
        --instance_created_at             		
        --instance_description,			
        --instance_emoji,			
        --instance_frontegg_tenant_id,			
        --instance_labelers_per_item,			
        --instance_multi_label,			
        --instance_name,			
        --instance_owner_id                			
        --instance_status,			
        --instance_template,			
        --instance_type,			
        --instance_updated_at              			
        --loaded_at			
        --original_timestamp			
        --received_at			
        --sent_at			
        timestamp                              AS event_date,			
        user_id			
        --uuid_ts			   
    FROM 
        source

)

SELECT *
FROM renamed