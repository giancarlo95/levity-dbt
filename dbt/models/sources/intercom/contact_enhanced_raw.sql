WITH source AS (

    SELECT 
        * 
    FROM 
        {{ source('dbt_giancarlo_intercom', 'intercom__contact_enhanced') }}

)

SELECT 
    contact_id,	
    admin_id,		
    created_at,			
    updated_at,			
    signed_up_at,			
    contact_name,
    contact_role,			
    contact_email,
    REGEXP_EXTRACT(contact_email, r"^[a-zA-Z0-9_.+-]+@([a-zA-Z0-9-]+).[a-zA-Z0-9-.]+$")     AS contact_domain,
    last_replied_at,			
    last_email_clicked_at,			
    last_email_opened_at,			
    last_contacted_at,			
    is_unsubscribed_from_emails,		
    custom_utm_campaign,		
    custom_utm_term,			
    custom_utm_source,			
    custom_utm_medium,			
    custom_utm_content,			
    custom_referral_url,			
    custom_lead_score,		
    custom_company_size,			
    custom_company_industry,			
    custom_organization_type,			
    custom_type_of_data,			
    custom_use_case_ai_block,			
    custom_use_case_pattern,			
    custom_use_case,			
    custom_ai_type,			
    custom_employee_role,			
    custom_tool_experience,			
    custom_source,
    custom_status,			
    custom_sentiment,			
    custom_account_approved,		
    latest_contact_index,		
    all_contact_tags,
    all_contact_company_names	
FROM 
    source
