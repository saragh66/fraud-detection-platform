{{ config(materialized='table') }}

SELECT
    merchant,
    category,
    COUNT(*) as total_transactions,
    SUM(is_fraud) as total_frauds,
    ROUND(SUM(amt), 2) as total_amount,
    ROUND(SUM(is_fraud) * 100.0 / COUNT(*), 2) as fraud_rate
FROM {{ source('fraud_detection', 'transactions') }}
GROUP BY merchant, category
ORDER BY total_frauds DESC
LIMIT 50