{{ config(
    materialized='ephemeral',
    tags = ['s89_itm_hrcy_grandmaster','s89_200_itm_hrcy_assn','s89_200_itm_hrcy_assn_intermediate']
) }}


    SELECT
        main.*,
        lookup6.ITM_HRCY_PRNT_NODE_ID AS  ITM_HRCY_PRNT_NODE_ID_1 
    FROM
        {{ source('Item_Vista_Schema','hrcy_lookup_psv_tFileInputDelimited_1') }} AS main 
    INNER JOIN
        {{ source('Item_Vista_Schema','hrcy_lookup_psv_tFileInputDelimited_2') }} AS lookup6 
        ON cast(main.ITM_HRCY_NODE_ID as STRING) = cast(lookup6.ITM_HRCY_NODE_ID as STRING)
    LEFT OUTER JOIN
        {{ ref('intermediate_item_hrcy_mstr_tmap_hash') }} AS lookup3
        ON cast(main.ITM_HRCY_NODE_ID as STRING) = cast(lookup3.HIERARCHY_NODE_ID as STRING)