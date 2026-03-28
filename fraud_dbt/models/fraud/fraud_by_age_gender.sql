{{ config(materialized='table') }}

SELECT
    gender,
    FLOOR(DATE_DIFF(CURRENT_DATE(), PARSE_DATE('%Y-%m-%d', dob), YEAR) / 10) * 10 as age_group,
    COUNT(*) as total_transactions,
    SUM(is_fraud) as total_frauds,
    ROUND(SUM(amt), 2) as total_amount,
    ROUND(SUM(is_fraud) * 100.0 / COUNT(*), 2) as fraud_rate
FROM {{ source('fraud_detection', 'transactions') }}
GROUP BY gender, age_group
ORDER BY age_group