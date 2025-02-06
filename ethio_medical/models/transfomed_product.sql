-- models/transformed_product_images.sql
{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        channel_id,
        message_id as product_id,
        product_name,
        price_in_birr  -- Rename the column to be clearer
    FROM {{ source('medical_data', 'transformed_product') }}
)

SELECT   -- Ensure unique channel usernames
    channel_id,
    product_id,
    product_name,
    price_in_birr  -- Rename the column to be clearer
FROM source_data