WITH legacy_users AS (

    SELECT
       user_id,
       user_email_address
    FROM 
       {{ref('legacy_users')}}

), django_production_users AS (

    SELECT 
        id                    AS user_id,
        email                 AS user_email_address 
    FROM
        {{ref('django_production_users')}}
    --WHERE 
        --email="thilo+selfmade-energy@levity.ai" OR NOT(email LIKE "%levity.ai")
)

SELECT * FROM legacy_users UNION DISTINCT
SELECT * FROM django_production_users
