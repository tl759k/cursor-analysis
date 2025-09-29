USE WAREHOUSE ADHOC;

-- Calculate individual dasher base pay deltas across all weeks
WITH dasher_deltas AS (
    SELECT 
        d.dasher_id,
        d.variant,
        -- Calculate total base pay delta for each dasher across all weeks
        (d.total_base_pay_w1 + d.total_base_pay_w2 + d.total_base_pay_w3 + d.total_base_pay_w4 + 
         d.total_base_pay_w5 + d.total_base_pay_w6 + d.total_base_pay_w7 + d.total_base_pay_w8) -
        (c.avg_base_pay_total * (d.total_delivs_w1 + d.total_delivs_w2 + d.total_delivs_w3 + d.total_delivs_w4 + 
         d.total_delivs_w5 + d.total_delivs_w6 + d.total_delivs_w7 + d.total_delivs_w8)) AS total_base_pay_delta_9weeks
    FROM henryliao.temp4 d
    CROSS JOIN (
        -- Get control group average base pay across all weeks
        SELECT 
            (SUM(total_base_pay_w1 + total_base_pay_w2 + total_base_pay_w3 + total_base_pay_w4 + 
                 total_base_pay_w5 + total_base_pay_w6 + total_base_pay_w7 + total_base_pay_w8) / 
             SUM(total_delivs_w1 + total_delivs_w2 + total_delivs_w3 + total_delivs_w4 + 
                 total_delivs_w5 + total_delivs_w6 + total_delivs_w7 + total_delivs_w8)) AS avg_base_pay_total
        FROM henryliao.temp4 
        WHERE variant = '0c'
    ) c
    WHERE d.variant != '0c'
),

-- Identify top 80% dashers (20th percentile and above) by base pay delta within each variant
top_dashers AS (
    SELECT 
        dasher_id,
        variant,
        total_base_pay_delta_9weeks,
        PERCENT_RANK() OVER (PARTITION BY variant ORDER BY total_base_pay_delta_9weeks DESC) as pct_rank
    FROM dasher_deltas
),

-- Create summary for overall and top 80% (20th percentile and above) groups
summary_by_group as (
    SELECT 
        variant,
        'overall' as group_type,
        COUNT(DISTINCT dasher_id) AS num_dasher,

        -- w1
        AVG(online_hours_w1) AS online_hours_w1,
        AVG(is_wad_w1) AS is_wad_w1,
        AVG(total_delivs_w1) AS total_delivs_w1,
        SUM(total_base_pay_w1) / SUM(total_delivs_w1) AS avg_base_pay_w1,

        -- W2
        AVG(online_hours_W2) AS online_hours_W2,
        AVG(is_wad_W2) AS is_wad_W2,
        AVG(total_delivs_W2) AS total_delivs_W2,
        SUM(total_base_pay_W2) / SUM(total_delivs_W2) AS avg_base_pay_W2,

        -- W3
        AVG(online_hours_W3) AS online_hours_W3,
        AVG(is_wad_W3) AS is_wad_W3,
        AVG(total_delivs_W3) AS total_delivs_W3,
        SUM(total_base_pay_W3) / SUM(total_delivs_W3) AS avg_base_pay_W3,

        -- W4
        AVG(online_hours_W4) AS online_hours_W4,
        AVG(is_wad_W4) AS is_wad_W4,
        AVG(total_delivs_W4) AS total_delivs_W4,
        SUM(total_base_pay_W4) / SUM(total_delivs_W4) AS avg_base_pay_W4,

        -- W5
        AVG(online_hours_W5) AS online_hours_W5,
        AVG(is_wad_W5) AS is_wad_W5,
        AVG(total_delivs_W5) AS total_delivs_W5,
        SUM(total_base_pay_W5) / SUM(total_delivs_W5) AS avg_base_pay_W5,

        -- W6
        AVG(online_hours_W6) AS online_hours_W6,
        AVG(is_wad_W6) AS is_wad_W6,
        AVG(total_delivs_W6) AS total_delivs_W6,
        SUM(total_base_pay_W6) / SUM(total_delivs_W6) AS avg_base_pay_W6,

        -- W7
        AVG(online_hours_W7) AS online_hours_W7,
        AVG(is_wad_W7) AS is_wad_W7,
        AVG(total_delivs_W7) AS total_delivs_W7,
        SUM(total_base_pay_W7) / SUM(total_delivs_W7) AS avg_base_pay_W7,

        -- W8
        AVG(online_hours_W8) AS online_hours_W8,
        AVG(is_wad_W8) AS is_wad_W8,
        AVG(total_delivs_W8) AS total_delivs_W8,
        SUM(total_base_pay_W8) / SUM(total_delivs_W8) AS avg_base_pay_W8
    FROM henryliao.temp4
    GROUP BY 1, 2

    UNION ALL

    SELECT 
        t.variant,
        'top_80_percent' as group_type,
        COUNT(DISTINCT t.dasher_id) AS num_dasher,

        -- w1
        AVG(t.online_hours_w1) AS online_hours_w1,
        AVG(t.is_wad_w1) AS is_wad_w1,
        AVG(t.total_delivs_w1) AS total_delivs_w1,
        SUM(t.total_base_pay_w1) / SUM(t.total_delivs_w1) AS avg_base_pay_w1,

        -- W2
        AVG(t.online_hours_W2) AS online_hours_W2,
        AVG(t.is_wad_W2) AS is_wad_W2,
        AVG(t.total_delivs_W2) AS total_delivs_W2,
        SUM(t.total_base_pay_W2) / SUM(t.total_delivs_W2) AS avg_base_pay_W2,

        -- W3
        AVG(t.online_hours_W3) AS online_hours_W3,
        AVG(t.is_wad_W3) AS is_wad_W3,
        AVG(t.total_delivs_W3) AS total_delivs_W3,
        SUM(t.total_base_pay_W3) / SUM(t.total_delivs_W3) AS avg_base_pay_W3,

        -- W4
        AVG(t.online_hours_W4) AS online_hours_W4,
        AVG(t.is_wad_W4) AS is_wad_W4,
        AVG(t.total_delivs_W4) AS total_delivs_W4,
        SUM(t.total_base_pay_W4) / SUM(t.total_delivs_W4) AS avg_base_pay_W4,

        -- W5
        AVG(t.online_hours_W5) AS online_hours_W5,
        AVG(t.is_wad_W5) AS is_wad_W5,
        AVG(t.total_delivs_W5) AS total_delivs_W5,
        SUM(t.total_base_pay_W5) / SUM(t.total_delivs_W5) AS avg_base_pay_W5,

        -- W6
        AVG(t.online_hours_W6) AS online_hours_W6,
        AVG(t.is_wad_W6) AS is_wad_W6,
        AVG(t.total_delivs_W6) AS total_delivs_W6,
        SUM(t.total_base_pay_W6) / SUM(t.total_delivs_W6) AS avg_base_pay_W6,

        -- W7
        AVG(t.online_hours_W7) AS online_hours_W7,
        AVG(t.is_wad_W7) AS is_wad_W7,
        AVG(t.total_delivs_W7) AS total_delivs_W7,
        SUM(t.total_base_pay_W7) / SUM(t.total_delivs_W7) AS avg_base_pay_W7,

        -- W8
        AVG(t.online_hours_W8) AS online_hours_W8,
        AVG(t.is_wad_W8) AS is_wad_W8,
        AVG(t.total_delivs_W8) AS total_delivs_W8,
        SUM(t.total_base_pay_W8) / SUM(t.total_delivs_W8) AS avg_base_pay_W8
    FROM henryliao.temp4 t
    INNER JOIN top_dashers td ON t.dasher_id = td.dasher_id AND t.variant = td.variant
    WHERE td.pct_rank <= 0.80  -- Top 80% (20th percentile and above)
    GROUP BY 1, 2

    UNION ALL

    -- Add control group for comparison
    SELECT 
        '0c' as variant,
        'control' as group_type,
        COUNT(DISTINCT dasher_id) AS num_dasher,

        -- w1
        AVG(online_hours_w1) AS online_hours_w1,
        AVG(is_wad_w1) AS is_wad_w1,
        AVG(total_delivs_w1) AS total_delivs_w1,
        SUM(total_base_pay_w1) / SUM(total_delivs_w1) AS avg_base_pay_w1,

        -- W2
        AVG(online_hours_W2) AS online_hours_W2,
        AVG(is_wad_W2) AS is_wad_W2,
        AVG(total_delivs_W2) AS total_delivs_W2,
        SUM(total_base_pay_W2) / SUM(total_delivs_W2) AS avg_base_pay_W2,

        -- W3
        AVG(online_hours_W3) AS online_hours_W3,
        AVG(is_wad_W3) AS is_wad_W3,
        AVG(total_delivs_W3) AS total_delivs_W3,
        SUM(total_base_pay_W3) / SUM(total_delivs_W3) AS avg_base_pay_W3,

        -- W4
        AVG(online_hours_W4) AS online_hours_W4,
        AVG(is_wad_W4) AS is_wad_W4,
        AVG(total_delivs_W4) AS total_delivs_W4,
        SUM(total_base_pay_W4) / SUM(total_delivs_W4) AS avg_base_pay_W4,

        -- W5
        AVG(online_hours_W5) AS online_hours_W5,
        AVG(is_wad_W5) AS is_wad_W5,
        AVG(total_delivs_W5) AS total_delivs_W5,
        SUM(total_base_pay_W5) / SUM(total_delivs_W5) AS avg_base_pay_W5,

        -- W6
        AVG(online_hours_W6) AS online_hours_W6,
        AVG(is_wad_W6) AS is_wad_W6,
        AVG(total_delivs_W6) AS total_delivs_W6,
        SUM(total_base_pay_W6) / SUM(total_delivs_W6) AS avg_base_pay_W6,

        -- W7
        AVG(online_hours_W7) AS online_hours_W7,
        AVG(is_wad_W7) AS is_wad_W7,
        AVG(total_delivs_W7) AS total_delivs_W7,
        SUM(total_base_pay_W7) / SUM(total_delivs_W7) AS avg_base_pay_W7,

        -- W8
        AVG(online_hours_W8) AS online_hours_W8,
        AVG(is_wad_W8) AS is_wad_W8,
        AVG(total_delivs_W8) AS total_delivs_W8,
        SUM(total_base_pay_W8) / SUM(total_delivs_W8) AS avg_base_pay_W8
    FROM henryliao.temp4
    WHERE variant = '0c'
    GROUP BY 1, 2
)

SELECT
    *
FROM (
    -- Week 1 results
    SELECT 
        t.variant,
        t.group_type,
        1 AS week,
        t.total_delivs_w1 * (t.avg_base_pay_w1 - c.avg_base_pay_w1) AS total_base_pay_delta,
        t.online_hours_w1 - c.online_hours_w1 AS online_hours_delta,
        online_hours_delta / c.online_hours_w1 AS online_hours_delta_rel,
        t.is_wad_w1 - c.is_wad_w1 AS is_wad_delta,
        total_base_pay_delta / online_hours_delta AS CPIH,
        total_base_pay_delta / is_wad_delta AS CPIWAD
    FROM summary_by_group t
    JOIN summary_by_group c ON c.variant = '0c' AND c.group_type = 'control'
    WHERE t.variant != '0c'

    UNION ALL 

    -- Week 2 results
    SELECT 
        t.variant,
        t.group_type,
        2 AS week,
        t.total_delivs_w2 * (t.avg_base_pay_w2 - c.avg_base_pay_w2) AS total_base_pay_delta,
        t.online_hours_w2 - c.online_hours_w2 AS online_hours_delta,
        online_hours_delta / c.online_hours_w2 AS online_hours_delta_rel,
        t.is_wad_w2 - c.is_wad_w2 AS is_wad_delta,
        total_base_pay_delta / online_hours_delta AS CPIH,
        total_base_pay_delta / is_wad_delta AS CPIWAD
    FROM summary_by_group t
    JOIN summary_by_group c ON c.variant = '0c' AND c.group_type = 'control'
    WHERE t.variant != '0c'

    UNION ALL 

    -- Week 3 results
    SELECT 
        t.variant,
        t.group_type,
        3 AS week,
        t.total_delivs_w3 * (t.avg_base_pay_w3 - c.avg_base_pay_w3) AS total_base_pay_delta,
        t.online_hours_w3 - c.online_hours_w3 AS online_hours_delta,
        online_hours_delta / c.online_hours_w3 AS online_hours_delta_rel,
        t.is_wad_w3 - c.is_wad_w3 AS is_wad_delta,
        total_base_pay_delta / online_hours_delta AS CPIH,
        total_base_pay_delta / is_wad_delta AS CPIWAD
    FROM summary_by_group t
    JOIN summary_by_group c ON c.variant = '0c' AND c.group_type = 'control'
    WHERE t.variant != '0c'

    UNION ALL 

    -- Week 4 results
    SELECT 
        t.variant,
        t.group_type,
        4 AS week,
        t.total_delivs_w4 * (t.avg_base_pay_w4 - c.avg_base_pay_w4) AS total_base_pay_delta,
        t.online_hours_w4 - c.online_hours_w4 AS online_hours_delta,
        online_hours_delta / c.online_hours_w4 AS online_hours_delta_rel,
        t.is_wad_w4 - c.is_wad_w4 AS is_wad_delta,
        total_base_pay_delta / online_hours_delta AS CPIH,
        total_base_pay_delta / is_wad_delta AS CPIWAD
    FROM summary_by_group t
    JOIN summary_by_group c ON c.variant = '0c' AND c.group_type = 'control'
    WHERE t.variant != '0c'

    UNION ALL 

    -- Week 5 results
    SELECT 
        t.variant,
        t.group_type,
        5 AS week,
        t.total_delivs_w5 * (t.avg_base_pay_w5 - c.avg_base_pay_w5) AS total_base_pay_delta,
        t.online_hours_w5 - c.online_hours_w5 AS online_hours_delta,
        online_hours_delta / c.online_hours_w5 AS online_hours_delta_rel,
        t.is_wad_w5 - c.is_wad_w5 AS is_wad_delta,
        total_base_pay_delta / online_hours_delta AS CPIH,
        total_base_pay_delta / is_wad_delta AS CPIWAD
    FROM summary_by_group t
    JOIN summary_by_group c ON c.variant = '0c' AND c.group_type = 'control'
    WHERE t.variant != '0c'

    UNION ALL 

    -- Week 6 results
    SELECT 
        t.variant,
        t.group_type,
        6 AS week,
        t.total_delivs_w6 * (t.avg_base_pay_w6 - c.avg_base_pay_w6) AS total_base_pay_delta,
        t.online_hours_w6 - c.online_hours_w6 AS online_hours_delta,
        online_hours_delta / c.online_hours_w6 AS online_hours_delta_rel,
        t.is_wad_w6 - c.is_wad_w6 AS is_wad_delta,
        total_base_pay_delta / online_hours_delta AS CPIH,
        total_base_pay_delta / is_wad_delta AS CPIWAD
    FROM summary_by_group t
    JOIN summary_by_group c ON c.variant = '0c' AND c.group_type = 'control'
    WHERE t.variant != '0c'

    UNION ALL 

    -- Week 7 results
    SELECT 
        t.variant,
        t.group_type,
        7 AS week,
        t.total_delivs_w7 * (t.avg_base_pay_w7 - c.avg_base_pay_w7) AS total_base_pay_delta,
        t.online_hours_w7 - c.online_hours_w7 AS online_hours_delta,
        online_hours_delta / c.online_hours_w7 AS online_hours_delta_rel,
        t.is_wad_w7 - c.is_wad_w7 AS is_wad_delta,
        total_base_pay_delta / online_hours_delta AS CPIH,
        total_base_pay_delta / is_wad_delta AS CPIWAD
    FROM summary_by_group t
    JOIN summary_by_group c ON c.variant = '0c' AND c.group_type = 'control'
    WHERE t.variant != '0c'

    UNION ALL 

    -- Week 8 results
    SELECT 
        t.variant,
        t.group_type,
        8 AS week,
        t.total_delivs_w8 * (t.avg_base_pay_w8 - c.avg_base_pay_w8) AS total_base_pay_delta,
        t.online_hours_w8 - c.online_hours_w8 AS online_hours_delta,
        online_hours_delta / c.online_hours_w8 AS online_hours_delta_rel,
        t.is_wad_w8 - c.is_wad_w8 AS is_wad_delta,
        total_base_pay_delta / online_hours_delta AS CPIH,
        total_base_pay_delta / is_wad_delta AS CPIWAD
    FROM summary_by_group t
    JOIN summary_by_group c ON c.variant = '0c' AND c.group_type = 'control'
    WHERE t.variant != '0c'
)
ORDER BY variant, group_type, week