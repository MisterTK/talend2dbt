{{ config(
    materialized='table',
    schema='RMDW_INT',
    alias='restreptmosls_unique',
    tags=['glma_s19_wk_mo_agg_wk_agg_grandmaster','glma_s19_wk_mo_agg_110_generate_context']
) }}
WITH input_data AS (
    SELECT
        CAST(YR_NU AS INT64) AS YR_NU,
        CAST(MO_NU AS INT64) AS MO_NU
    FROM {{ source('RMDW_RAW','SLTC_LGCY_REST_REPT_MO_SLS') }}

    UNION ALL

    SELECT
        CAST(YR_NU AS INT64) AS YR_NU,
        CAST(MO_NU AS INT64) AS MO_NU
    FROM {{ source('RMDW_RAW','SLTC_GBL_REST_REPT_MO_SLS') }}
),
parsed_dates AS (
    SELECT
        "X" AS DUMMY_KEY,
        PARSE_DATE(
            '%Y-%m-%d',
            CAST(YR_NU AS STRING) || '-' || LPAD(CAST(MO_NU AS STRING), 2, '0') || '-01'
        ) AS MAX_SLTC_DT
    FROM input_data
),
ranked AS (
    SELECT
        DUMMY_KEY,
        MAX_SLTC_DT,
        ROW_NUMBER() OVER (
            PARTITION BY DUMMY_KEY
            ORDER BY DUMMY_KEY ASC, MAX_SLTC_DT DESC
        ) AS rn
    FROM parsed_dates
)
SELECT
    DUMMY_KEY,
    MAX_SLTC_DT
FROM ranked
WHERE rn = 1
ORDER BY DUMMY_KEY ASC

