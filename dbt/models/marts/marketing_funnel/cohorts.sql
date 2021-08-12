WITH users AS (

    SELECT 
        date,
        previous_week_users
    FROM 
        {{ref('users')}}
    WHERE 
        EXTRACT(DAYOFWEEK FROM date)=7

), weekly_signups AS (

    SELECT 
        * 
    FROM 
        {{ref('weekly_signups')}}


), weekly_access_call_booked AS (

    SELECT 
        * 
    FROM 
        {{ref('weekly_access_call_booked')}}


), weekly_signups_form_completed AS (

    SELECT 
        * 
    FROM 
        {{ref('weekly_signups_form_completed')}}


)

SELECT 
    weekly_signups.year,
    weekly_signups.week_number,
    DATE_SUB(users.date, INTERVAL 6 DAY)                                                            AS first_week_day,
    users.date                                                                                      AS last_week_day,
    users.previous_week_users                                                                       AS traffic,
    weekly_signups.number_of_signups                                                                AS signups,
    weekly_signups_form_completed.number_of_forms                                                   AS forms_completed,
    weekly_access_call_booked.number_of_calls                                                       AS access_calls_booked
FROM users 
LEFT JOIN weekly_signups ON
    users.date=weekly_signups.week_end
LEFT JOIN weekly_signups_form_completed ON
    users.date=weekly_signups_form_completed.week_end
LEFT JOIN weekly_access_call_booked ON
    users.date=weekly_access_call_booked.week_end
WHERE 
    weekly_signups.year=2021 
    AND weekly_signups.week_number>=23
ORDER BY 
    weekly_signups.year,
    weekly_signups.week_number

