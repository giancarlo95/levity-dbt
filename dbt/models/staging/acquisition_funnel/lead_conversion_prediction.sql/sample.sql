WITH contact_enhanced AS (

    SELECT
        contact_email,
        COALESCE(location_country, "Unknown") AS location_country,
        ---COALESCE (custom_lead_score, AVG(custom_lead_score) OVER ()) AS lead_score,
        COALESCE (custom_lead_score, 0) AS lead_score,		
        custom_company_size,			
        custom_company_industry,			
        custom_organization_type,			
        custom_type_of_data,
        custom_use_case,			
        custom_status,
        CASE 
            WHEN custom_status IN ('Active in free plan', 'Integration needs', 'On Hold for Customer Process', 'POC Offer Made', 'Paid Plan', 'Process Kickoff') THEN 1
            ELSE 0
        END AS is_customer,			
        custom_employee_role,			
        custom_tool_experience,			
        all_contact_company_names,
        CASE 
            WHEN all_contact_company_names IS NOT NULL THEN ROW_NUMBER() OVER (PARTITION BY all_contact_company_names)
            ELSE 1
        END AS screen
    FROM 
       {{ref("contact_enhanced")}}
    WHERE 
        contact_role="user"
        AND custom_type_of_data IS NOT NULL
    ORDER BY
        contact_email

), company_enhanced AS (

    SELECT
        company_name,
        ROW_NUMBER() OVER (PARTITION BY company_name ORDER BY custom_company_industry_list) AS index,
        custom_company_industry_list,		
        custom_company_organization_type,		
        custom_company_size_list
    FROM 
       {{ref("company_enhanced")}}

), company_enhanced_unique AS (

    SELECT 
        *
    FROM 
        company_enhanced
    WHERE 
        index=1

), final AS (

    SELECT 
        ROW_NUMBER() OVER() AS index,
        CASE
            WHEN location_country IN ("Germany", "United Kingdom", "United States") THEN location_country
            WHEN location_country IN ("Croatia", "Israel", "Portugal", "Spain", "Sweden", "Belgium", "Italy", "Estonia", "Switzerland", "Hungary", "Austria", "Bulgaria", "France", "Ukraine", "Slovakia", "Czechia", "Netherlands", "Latvia", "Ireland", "Romania", "Turkey", "Russia") THEN "Europe"
            WHEN location_country="Unknown" THEN location_country
            ELSE "Other"
        END AS location_country_cat,
        lead_score,		
        CASE
            WHEN contact_enhanced.custom_company_size IS NULL THEN COALESCE (company_enhanced_unique.custom_company_size_list, 'Unknown')
            ELSE contact_enhanced.custom_company_size
        END AS company_size,	
        CASE
            WHEN contact_enhanced.custom_company_industry IS NULL THEN COALESCE (company_enhanced_unique.custom_company_industry_list, 'Unknown')
            ELSE contact_enhanced.custom_company_industry
        END AS company_industry,	
        CASE
            WHEN contact_enhanced.custom_organization_type IS NULL THEN COALESCE (company_enhanced_unique.custom_company_organization_type, 'Unknown')
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
        CASE 
            WHEN LOWER(custom_use_case) LIKE '%classif%' OR LOWER(custom_use_case) LIKE '%categor%' OR LOWER(custom_use_case) LIKE '%detect%' OR LOWER(custom_use_case) LIKE '%priorit%' THEN 1
            ELSE 0
        END AS is_promising,
        is_customer
    FROM
        contact_enhanced LEFT JOIN company_enhanced_unique 
        ON contact_enhanced.all_contact_company_names=company_enhanced_unique.company_name
    WHERE 
        screen=1

)

SELECT 
    *,
    CASE 
        WHEN company_size IN ('Just me', 'Just Me') THEN 'Alone'
        WHEN company_size IN ('2-10') THEN 'Very Small'
        WHEN company_size IN ('11-50') THEN 'Small'
        WHEN company_size IN ('51-250') THEN 'Small Medium'
        WHEN company_size IN ('251-1000','251-1,000') THEN 'Medium'
        WHEN company_size IN ('1000-5000','1,000-5,000') THEN 'Medium Large'
        WHEN company_size IN ('5000+','5,000+') THEN 'Large'
        ELSE 'Unknown'
    END AS company_size_refactored
FROM
    final