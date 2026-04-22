WITH
    months AS (
        SELECT DISTINCT
            metric_month
        FROM
            {{ref ('stg_twitch')}}
        WHERE
            metric_month IS NOT NULL
    )
SELECT
    (year(metric_month) * 100 + month(metric_month))::INT AS date_key,
    metric_month AS full_date,
    year(metric_month) AS year,
    month(metric_month) AS month,
    monthname(metric_month) AS month_name,
    quarter(metric_month) AS quarter
FROM
    months
ORDER BY
    full_date