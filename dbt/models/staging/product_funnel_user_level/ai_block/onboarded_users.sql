WITH auth_user AS (

    SELECT
       user_id,
       username,
       user_email_address,
       date_user_onboarded 
    FROM 
       {{ref('auth_user')}}
    WHERE
       is_staff=FALSE

), accounts_userprofile AS (

    SELECT 
       user_id
    FROM 
       {{ref('accounts_userprofile')}}
    WHERE 
       is_approved=TRUE

), user_table AS (

    SELECT
       user_id,
       user_name,
       user_email_address,
       date_user_onboarded
    FROM 
       {{ref('user_table')}}
    WHERE 
        is_locked=FALSE

), auth_user_filtered AS (

    SELECT 
        auth_user.user_id,
        username,
        user_email_address,
        date_user_onboarded
    FROM auth_user 
    INNER JOIN accounts_userprofile ON 
        auth_user.user_id=accounts_userprofile.user_id 

), final AS (

    SELECT 
        * 
    FROM user_table 
    WHERE 
       NOT(user_email_address LIKE '%@angularventures.com' OR user_email_address LIKE '%@discovery-ventures.com' OR user_email_address LIKE '%@levity.ai')
       AND NOT(user_email_address IN ('judithwollers@gmail.com', 'abcaisodjaiosdjioasd@gmail.com', 'adil.islam619@gmail.com', 'milanjose999@gmail.com', 'hanna.kleinings@gmail.com', 'aymenbenothmenabo@gmail.com'))
    UNION ALL
    SELECT 
        * 
    FROM auth_user_filtered
    WHERE 
       NOT(user_email_address LIKE '%@angularventures.com' OR user_email_address LIKE '%@discovery-ventures.com' OR user_email_address LIKE '%@levity.ai')
       AND NOT(user_email_address IN ('judithwollers@gmail.com', 'abcaisodjaiosdjioasd@gmail.com', 'adil.islam619@gmail.com', 'milanjose999@gmail.com', 'hanna.kleinings@gmail.com', 'aymenbenothmenabo@gmail.com'))

), final_dedup AS (

    SELECT 
        DISTINCT(user_email_address)
    FROM 
        final

), final_large AS (

    SELECT 
        final_dedup.user_email_address,
        user_table.user_id                          AS user_id,
        user_table.user_name,
        auth_user.user_id                           AS old_user_id,
        CASE 
            WHEN auth_user.date_user_onboarded IS NULL THEN user_table.date_user_onboarded
            ELSE auth_user.date_user_onboarded
        END                                         AS date_user_onboarded
    FROM 
        final_dedup 
    LEFT JOIN 
        user_table ON user_table.user_email_address=final_dedup.user_email_address
    LEFT JOIN 
        auth_user ON auth_user.user_email_address=final_dedup.user_email_address

), datasets_dataset AS (

    SELECT 
       DISTINCT user_id, old_user_id
    FROM 
       {{ref('datasets_dataset')}}

), final_new AS (

    SELECT 
        LOWER(final_large.user_email_address) AS user_email_address,
        final_large.user_name,
        final_large.date_user_onboarded,
        CASE 
            WHEN (final_large.user_id IS NULL) AND (datasets_dataset.user_id IS NOT NULL) THEN datasets_dataset.user_id
            WHEN (final_large.user_id IS NULL) AND (datasets_dataset.user_id IS NULL) THEN final_large.old_user_id
            ELSE final_large.user_id
        END                                                                                                                    AS user_id    
    FROM final_large
    LEFT JOIN datasets_dataset ON datasets_dataset.old_user_id=final_large.old_user_id

),  accounts_userprofile_excluded AS (

    SELECT 
       user_id
    FROM 
       {{ref('accounts_userprofile')}}
    WHERE 
       is_approved=FALSE

), auth_user_excluded AS (

    SELECT 
        auth_user.user_id,
        username,
        user_email_address,
        date_user_onboarded,
        "excluded"                 AS flag
    FROM auth_user 
    INNER JOIN accounts_userprofile_excluded ON 
        auth_user.user_id=accounts_userprofile_excluded.user_id 
 
)

SELECT 
    final_new.user_id,
    final_new.user_name,
    final_new.user_email_address,
    final_new.date_user_onboarded,
    "Onboarded" AS flag
FROM final_new
LEFT JOIN auth_user_excluded ON final_new.user_email_address=auth_user_excluded.user_email_address
WHERE auth_user_excluded.flag  IS NULL

