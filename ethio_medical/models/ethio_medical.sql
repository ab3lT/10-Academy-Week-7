-- models/ethio_medical.sql
{{ config(materialized='table') }}  -- Correcting the materialization syntax

WITH source_data AS (
    SELECT * 
    FROM {{ source('medical_data', 'ethio_medical') }}  -- Use DBT's source function
)

SELECT
    channel_id,
    channel_title,
    lower(channel_username) AS channel_username,
    message_id,
    "Message",
    date,
    media_path
FROM source_data
WHERE "Message" IS NOT NULL