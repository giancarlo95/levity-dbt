{{
  config(
    materialized = 'table',
    )
}}

WITH accounts_paymentplan AS (

    SELECT 
        *,
        ROW_NUMBER() OVER(PARTITION BY frontegg_user_id, frontegg_tenant_id ORDER BY updated_at DESC) AS index
    FROM
        {{ref("accounts_paymentplan")}}

)

SELECT 
    *
FROM
    accounts_paymentplan
WHERE 
    index=1

