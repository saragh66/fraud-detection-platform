{{ config(materialized='table') }}

SELECT
    category,
    COUNT(*) as total_transactions,
    SUM(is_fraud) as total_frauds,
    ROUND(SUM(amt), 2) as total_amount,
    ROUND(AVG(amt), 2) as avg_amount,
    ROUND(SUM(is_fraud) * 100.0 / COUNT(*), 2) as fraud_rate
FROM {{ source('fraud_detection', 'transactions') }}
GROUP BY category
ORDER BY total_frauds DESC