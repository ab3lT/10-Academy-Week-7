{{ config(materialized='table') }}

WITH source_data AS (
    SELECT * 
    FROM {{ source('medical_data', 'transformed_medical_product') }}
),
phone_extracted AS (
    SELECT
        channel_id,
        message_id,
        channel_title,
        lower(channel_username) AS channel_username,
        "Message",
        date,
        media_path,  -- Ensure this is being selected
        -- Extract phone numbers with optional spaces
        array_to_string(ARRAY(
            SELECT regexp_replace(unnest(regexp_matches("Message", '09\s*[0-9]{8}', 'g')), '\s+', '', 'g')
        ), ', ') AS phone_numbers,
        -- Clean the message of phone numbers
        regexp_replace(
            "Message", 
            '09\s*[0-9]{8}', 
            '', 
            'g'
        ) AS cleaned_message
    FROM source_data
),
product_price_extracted AS (
    SELECT
        channel_id,
        message_id,
        channel_title,
        channel_username,
        date,
        TRIM(cleaned_message) AS cleaned_message,
        phone_numbers,
        -- Adjusted regex to capture product names and prices
        regexp_matches(cleaned_message, '^(.*?)\s*(?:price|Price|PRICE)\s*(\d+)\s*(birr|ETB)', 'g') AS matches,
        media_path,  -- Include media_path here
        ROW_NUMBER() OVER (PARTITION BY message_id ORDER BY date DESC) AS rn  -- Add row number to filter
    FROM phone_extracted
)

SELECT
    channel_id,
    message_id,
    channel_title,
    channel_username,
    date,
    TRIM(matches[1]) AS product_name,  -- Extract product name
    CAST(TRIM(matches[2]) AS INTEGER) AS price_in_birr,  -- Extract price as an integer
    media_path,  -- Include media_path in the final selection
    CASE
        WHEN phone_numbers IS NOT NULL AND phone_numbers != '' THEN phone_numbers
        ELSE NULL
    END AS contact_phone_numbers  -- Renamed for clarity
FROM product_price_extracted
WHERE matches IS NOT NULL  -- Filter out any rows where matches are not found
AND rn = 1  -- Select only the first occurrence of each message_id
AND TRIM(matches[1]) <> ''  -- Drop empty product names
AND TRIM(matches[1]) <> TRIM(matches[2])  -- Drop rows where product name and price are the same
ORDER BY message_id  -- Optional: order by message_id