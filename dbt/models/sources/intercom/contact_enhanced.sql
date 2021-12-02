WITH source AS (

    SELECT 
        * 
    FROM 
        {{ source('dbt_giancarlo_intercom', 'intercom__contact_enhanced') }}

), all_contacts AS (

    SELECT 
        *
    FROM 
        {{ ref('all_contacts') }}

), final AS (

    SELECT 
        source.contact_id,	
        admin_id,		
        source.created_at,			
        updated_at,			
        CASE 
            WHEN signed_up_at IS NULL AND contact_role='user' THEN source.created_at
            ELSE signed_up_at
        END                                                                         AS signed_up_at,			
        contact_name,
        REGEXP_EXTRACT(contact_name, r"[^\s]+")                                     AS contact_first_name,
        REGEXP_REPLACE(contact_name, REGEXP_EXTRACT(contact_name, r"[^\s]+"), "")	AS contact_last_name,		
        CASE
            WHEN all_contact_tags LIKE "%lead%" THEN "lead"
            ELSE contact_role
        END                                                                         AS contact_role,			
        source.contact_email,
        --CASE 
            --WHEN source.contact_email LIKE "%@levity.ai%" THEN 1
            --ELSE 0   
        --END                                                                        AS residual,
        CASE 
            WHEN source.contact_email IS NOT NULL THEN ROW_NUMBER() OVER (PARTITION BY source.contact_email ORDER BY contact_role DESC)
            ELSE 1
        END                                                                         AS screen,
        last_replied_at,			
        last_email_clicked_at,			
        last_email_opened_at,			
        last_contacted_at,			
        is_unsubscribed_from_emails,
        location_country,		
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
        custom_status,			
        custom_employee_role,			
        custom_tool_experience,			
        custom_source,			
        custom_sentiment,			
        custom_account_approved,		
        latest_contact_index,		
        all_contact_tags,
        CASE 
            WHEN all_contact_tags LIKE '%Company: Levity%' THEN true
            ELSE false 
        END                                                                      AS internal_user,
        all_contact_company_names	
    FROM source
    INNER JOIN all_contacts ON all_contacts.contact_id=source.contact_id

) 

SELECT 
    *
FROM final
WHERE 
    internal_user=false 
    AND screen=1
    




