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


)

SELECT 
    weekly_signups.year,
    weekly_signups.week_number,
    users.date,
    users.previous_week_users                                                                       AS traffic,
    weekly_signups.number_of_signups                                                                AS signups,
    weekly_access_call_booked.number_of_calls                                                       AS access_calls_booked,
    weekly_signups.number_of_signups/users.previous_week_users                                      AS traffic_to_signups,
    weekly_access_call_booked.number_of_calls/weekly_signups.number_of_signups                      AS signups_to_access_calls,
FROM users 
LEFT JOIN weekly_signups ON
    users.date=weekly_signups.week_end
LEFT JOIN weekly_access_call_booked ON
    users.date=weekly_access_call_booked.week_end

