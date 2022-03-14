WITH legacy_users AS (

    SELECT
       user_id,
       user_email_address,
       date_user_onboarded    AS user_created_at
    FROM 
       {{ref('legacy_users')}}

), django_production_users AS (

    SELECT 
        id                    AS user_id,
        email                 AS user_email_address,
        received_at           AS user_created_at  
    FROM
        {{ref('django_production_users')}}
    WHERE 
        NOT(email LIKE "%levity.ai")
)

SELECT * FROM legacy_users UNION ALL
SELECT * FROM django_production_users
