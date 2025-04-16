-- time_series_data.sql
-- This model selects data from the time_series_data table in the lightdash_data database

{{ config(
    materialized='table'
) }}

SELECT
    "DATE" as date,
    "PRODUCT" as product,
    "REGION" as region,
    "SOURCE_CURRENCY" as source_currency,
    "TARGET_CURRENCY" as target_currency,
    "VOLUME" as volume,
    "ACTIVE_CUSTOMERS" as active_customers
FROM {{ source('lightdash_data', 'time_series_data') }}
