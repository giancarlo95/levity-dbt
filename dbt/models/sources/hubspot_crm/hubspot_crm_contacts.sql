{{
  config(
    materialized = 'table',
    )
}}

WITH source AS (

    SELECT 
        *
    FROM 
        {{ source('hubspot_crm', 'contacts_view')}}

), renamed AS (

    SELECT
        added_at,			
        canonical_vid,		
        email,			
        form_submissions,			
        id,			
        is_contact,		
        lead_guid,			
        list_memberships,			
        loaded_at,			
        merged_vids,			
        portal_id,			
        properties_company_value,			
        properties_firstname_value,			
        properties_lastmodifieddate_value,			
        properties_lastname_value,			
        received_at,			
        uuid_ts,			
        properties_app_signup_value,		
        properties_hs_analytics_source_value,	
        TIMESTAMP_MILLIS(CAST(properties_hs_lifecyclestage_lead_date_value AS INT64)) AS properties_hs_lifecyclestage_lead_date_value,			
        TIMESTAMP_MILLIS(CAST(properties_hs_lifecyclestage_marketingqualifiedlead_date_value AS INT64)) AS properties_hs_lifecyclestage_marketingqualifiedlead_date_value,			
        properties_lead_score_value,			
        properties_lifecyclestage_value,			
        TIMESTAMP_MILLIS(CAST(properties_sign_up_date_value AS INT64)) AS properties_sign_up_date_value,
        TIMESTAMP_MILLIS(CAST(properties_hs_email_last_reply_date_value AS INT64)) AS properties_hs_email_last_reply_date_value,
        TIMESTAMP_MILLIS(CAST(properties_hs_last_sales_activity_timestamp_value AS INT64)) AS properties_hs_last_sales_activity_timestamp_value,
        TIMESTAMP_MILLIS(CAST(properties_hs_sales_email_last_replied_value AS INT64)) AS properties_hs_sales_email_last_replied_value	
    FROM 
        source

)

SELECT *
FROM renamed