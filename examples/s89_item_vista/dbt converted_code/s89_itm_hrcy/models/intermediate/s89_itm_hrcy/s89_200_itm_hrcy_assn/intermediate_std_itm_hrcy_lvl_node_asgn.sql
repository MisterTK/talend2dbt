{{ config(
    materialized='table',
    tags = ['s89_itm_hrcy_grandmaster','s89_200_itm_hrcy_assn','s89_200_itm_hrcy_assn_intermediate']
) }}


SELECT
    itm_hrcy_lvl_nu,
    itm_hrcy_lvl_eff_dt,
    CASE
        WHEN NODE_EFFECTIVE_DATE IS NULL THEN PARSE_DATE('%Y-%m-%d', '9999-01-01')
        ELSE NODE_EFFECTIVE_DATE
    END AS lvl_node_asgn_eff_dt,
    itm_hrcy_node_id,
    CAST(NULL AS DATE) AS lvl_node_asgn_end_dt,
    DW_FILE_ID AS dw_file_id

FROM {{ ref('intermediate_item_hrcy_mstr_hrcy_lookup_tmap') }}