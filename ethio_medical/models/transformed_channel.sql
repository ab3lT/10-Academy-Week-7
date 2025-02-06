-- models/transformed_product_images.sql
{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        channel_id,
        channel_username,
        channel_title
    FROM {{ source('medical_data', 'transformed_medical_product') }}
    WHERE contact_phone_numbers IS NOT NULL AND contact_phone_numbers != ''  -- Filter out any null or empty paths
)

SELECT DISTINCT ON (channel_id)  -- Ensure unique channel usernames
    channel_id,
    channel_username,
    channel_title  -- Rename the column to be clearer
FROM source_data