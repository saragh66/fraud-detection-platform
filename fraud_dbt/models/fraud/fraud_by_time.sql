{{ config(materialized='table') }}

SELECT
    DATE(trans_date_trans_time) as date,
    EXTRACT(HOUR FROM trans_date_trans_time) as hour,
    EXTRACT(MONTH FROM trans_date_trans_time) as month,
    EXTRACT(DAYOFWEEK FROM trans_date_trans_time) as day_of_week,
    COUNT(*) as total_transactions,
    SUM(is_fraud) as total_frauds,
    ROUND(SUM(amt), 2) as total_amount,
    ROUND(SUM(is_fraud) * 100.0 / COUNT(*), 2) as fraud_rate
FROM {{ source('fraud_detection', 'transactions') }}
GROUP BY date, hour, month, day_of_week
ORDER BY date