WITH contact_enhanced AS (

    SELECT
        contact_email,
        custom_lead_score,		
        custom_company_size,			
        custom_company_industry,			
        custom_organization_type,			
        custom_type_of_data,			
        custom_status,			
        custom_employee_role,			
        custom_tool_experience,			
        all_contact_company_names
    FROM 
       {{ref("contact_enhanced")}}
    WHERE 
        contact_role="user"
        AND custom_type_of_data IS NOT NULL

), company_enhanced AS (

    SELECT
        company_name,
        custom_company_industry_list,		
        custom_company_organization_type,		
        custom_company_size_list
    FROM 
       {{ref("company_enhanced")}}

)

SELECT 
    contact_email,
    custom_lead_score AS lead_score,		
    CASE
        WHEN contact_enhanced.custom_company_size IS NULL THEN company_enhanced.custom_company_size_list 
        ELSE contact_enhanced.custom_company_size
    END AS company_size,	
    CASE
        WHEN contact_enhanced.custom_company_industry IS NULL THEN company_enhanced.custom_company_industry_list 
        ELSE contact_enhanced.custom_company_industry
    END AS company_industry,	
    CASE
        WHEN contact_enhanced.custom_organization_type IS NULL THEN company_enhanced.custom_company_organization_type 
        ELSE contact_enhanced.custom_organization_type
    END AS organization_type,				
    CASE 
        WHEN custom_type_of_data LIKE "%Image%" THEN 1
        ELSE 0
    END AS is_image,
    CASE 
        WHEN custom_type_of_data LIKE "%Text%" THEN 1
        ELSE 0
    END AS is_text,
    CASE 
        WHEN custom_type_of_data LIKE "%Document%" THEN 1
        ELSE 0
    END AS is_document,				
    CASE 
        WHEN custom_employee_role LIKE "%Founder%" THEN 1
        ELSE 0
    END AS is_founder,
    CASE 
        WHEN custom_employee_role LIKE "%Developer%" THEN 1
        ELSE 0
    END AS is_developer,
    CASE 
        WHEN custom_employee_role LIKE "%Marketing%" THEN 1
        ELSE 0
    END AS is_marketing,
    CASE 
        WHEN custom_employee_role LIKE "%Product%" THEN 1
        ELSE 0
    END AS is_product,	
    CASE 
        WHEN custom_employee_role LIKE "%Researcher%" THEN 1
        ELSE 0
    END AS is_researcher,
    CASE 
        WHEN custom_employee_role LIKE "%Operations%" THEN 1
        ELSE 0
    END AS is_operations,	
    CASE 
        WHEN custom_tool_experience LIKE "None" OR custom_tool_experience IS NULL THEN 0
        ELSE 1
    END AS is_tech_savvy,
    custom_status AS status
FROM
    contact_enhanced LEFT JOIN company_enhanced 
    ON contact_enhanced.all_contact_company_names=company_enhanced.company_name