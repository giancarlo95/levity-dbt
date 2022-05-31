{{
  config(
    materialized = 'incremental',
    unique_key = 'week_monday'
    )
}}

WITH health_score AS (

    SELECT 
        *
    FROM
        {{ref("health_score")}}

)

SELECT
    DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)) AS week_monday,
    EXTRACT(YEAR FROM CURRENT_DATE()) AS year,
    EXTRACT(WEEK(MONDAY) FROM CURRENT_DATE()) AS week, 
    MIN(overall_score) OVER() AS overall_score_min,
    PERCENTILE_CONT(overall_score, 0.1) OVER() AS overall_score_10th_perc,
    PERCENTILE_CONT(overall_score, 0.15) OVER() AS overall_score_15th_perc,
    PERCENTILE_CONT(overall_score, 0.2) OVER() AS overall_score_20th_perc,
    PERCENTILE_CONT(overall_score, 0.25) OVER() AS overall_score_25th_perc,
    PERCENTILE_CONT(overall_score, 0.3) OVER() AS overall_score_30th_perc,
    PERCENTILE_CONT(overall_score, 0.35) OVER() AS overall_score_35th_perc,
    PERCENTILE_CONT(overall_score, 0.4) OVER() AS overall_score_40th_perc,
    PERCENTILE_CONT(overall_score, 0.45) OVER() AS overall_score_45th_perc,
    PERCENTILE_CONT(overall_score, 0.5) OVER() AS overall_score_50th_perc,
    AVG(overall_score) OVER() AS overall_score_avg,
    PERCENTILE_CONT(overall_score, 0.55) OVER() AS overall_score_55th_perc,
    PERCENTILE_CONT(overall_score, 0.6) OVER() AS overall_score_60th_perc,
    PERCENTILE_CONT(overall_score, 0.65) OVER() AS overall_score_65th_perc,
    PERCENTILE_CONT(overall_score, 0.7) OVER() AS overall_score_70th_perc,
    PERCENTILE_CONT(overall_score, 0.75) OVER() AS overall_score_75th_perc,
    PERCENTILE_CONT(overall_score, 0.8) OVER() AS overall_score_80th_perc,
    PERCENTILE_CONT(overall_score, 0.85) OVER() AS overall_score_85th_perc,
    PERCENTILE_CONT(overall_score, 0.9) OVER() AS overall_score_90th_perc,
    PERCENTILE_CONT(overall_score, 0.95) OVER() AS overall_score_95th_perc,
    MAX(overall_score) OVER() AS overall_score_max
FROM
    health_score
LIMIT 1