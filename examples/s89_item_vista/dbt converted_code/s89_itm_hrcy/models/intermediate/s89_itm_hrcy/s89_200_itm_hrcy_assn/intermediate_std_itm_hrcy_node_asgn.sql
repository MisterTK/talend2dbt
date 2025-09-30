{{ config(
    materialized='table',
    tags = ['s89_itm_hrcy_grandmaster','s89_200_itm_hrcy_assn','s89_200_itm_hrcy_assn_intermediate']
) }}

SELECT
    itm_hrcy_node_id,
    CASE
        WHEN NODE_EFFECTIVE_DATE IS NULL THEN PARSE_DATE('%Y-%m-%d', '9999-01-01')
        ELSE NODE_EFFECTIVE_DATE
    END AS hrcy_node_asgn_eff_dt,
    ITM_HRCY_PRNT_NODE_ID_1 AS pren_itm_hrcy_node_id,
    CAST(NULL AS DATE) AS hrcy_node_asgn_end_dt,
    CURRENT_DATETIME as load_dw_audt_ts,
    CURRENT_DATETIME as updt_dw_audt_ts,
    DW_FILE_ID AS dw_file_id

FROM {{ ref('intermediate_item_hrcy_mstr_hrcy_lookup_tmap') }}
    