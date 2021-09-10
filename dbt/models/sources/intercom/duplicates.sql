WITH all_contacts AS (

    SELECT 
        * 
    FROM 
        {{ ref('all_contacts') }}

), contact_enhanced_raw AS (

    SELECT 
        *
    FROM 
        {{ ref('contact_enhanced_raw')}}      

), duplicate AS (

    SELECT 
        contact_name,
        contact_domain,
        COUNT(DISTINCT contact_id)
    FROM contact_enhanced_raw
    GROUP BY 
        contact_name,
        contact_domain
    HAVING
        COUNT(DISTINCT contact_id)>1

)

SELECT 
    contact_enhanced_raw.contact_id,	
    admin_id,		
    contact_enhanced_raw.created_at,			
    updated_at,			
    signed_up_at,			
    contact_enhanced_raw.contact_name,
    contact_role,			
    contact_enhanced_raw.contact_email,
    contact_enhanced_raw.contact_domain,
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
FROM contact_enhanced_raw
INNER JOIN duplicate ON duplicate.contact_name=contact_enhanced_raw.contact_name AND duplicate.contact_domain=contact_enhanced_raw.contact_domain
INNER JOIN all_contacts ON all_contacts.contact_id=contact_enhanced_raw.contact_id
