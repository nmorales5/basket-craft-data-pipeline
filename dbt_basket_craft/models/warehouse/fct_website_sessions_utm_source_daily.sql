WITH daily_sessions AS (
    SELECT
        DATE(website_session_created_at) AS website_session_day,
        utm_source,

        COUNT(*) AS sessions,
        SUM(CASE WHEN is_repeat_session = 1 THEN 1 ELSE 0 END) AS repeat_sessions,
        ROUND(
            SUM(CASE WHEN is_repeat_session = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
            2
        ) AS repeat_sessions_pct,

        CURRENT_TIMESTAMP AS loaded_at
    FROM {{ ref('stg_website_sessions') }}
    GROUP BY 1, 2
)
SELECT * FROM daily_sessions
ORDER BY website_session_day, utm_source
