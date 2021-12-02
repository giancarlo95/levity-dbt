WITH source AS (

    SELECT 
        * 
    FROM 
        {{ source('dbt_prod_intercom', 'intercom__company_enhanced') }}

)

SELECT 
    company_id,		
    company_name,		
    website,		
    industry,		
    created_at,			
    updated_at,			
    user_count,			
    session_count,			
    monthly_spend,			
    plan_id,		
    plan_name,		
    _fivetran_deleted,			
    custom_company_industry_list,		
    custom_company_organization_type,		
    custom_company_size_list,		
    all_company_tags
FROM source
WHERE 
    _fivetran_deleted=false