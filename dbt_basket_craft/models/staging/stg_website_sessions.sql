WITH raw_sessions AS (
    SELECT * FROM {{ source('basket_craft', 'website_sessions') }}
),
stg_sessions AS (
    SELECT
        website_session_id,
        created_at AS website_session_created_at,
        user_id,
        utm_source,
        utm_campaign,
        device_type,
        is_repeat_session,
        CURRENT_TIMESTAMP AS loaded_at
    FROM raw_sessions
)
SELECT * FROM stg_sessions
