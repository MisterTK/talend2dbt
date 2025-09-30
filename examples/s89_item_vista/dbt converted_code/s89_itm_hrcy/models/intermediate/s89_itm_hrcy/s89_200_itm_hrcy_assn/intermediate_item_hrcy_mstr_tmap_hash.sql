{{ config(
    materialized='ephemeral',
    tags = ['s89_itm_hrcy_grandmaster','s89_200_itm_hrcy_assn','s89_200_itm_hrcy_assn_intermediate']
) }}

SELECT
    main.TERRITORY_CODE,
    main.SMI_ID,
    main.GLOBAL_MENU_IDENTIFIER,
    main.HIERARCHY_NAME,
    main.HIERARCHY_NODE_ID,
    main.EFFECTIVE_DATE,
    main.DW_FILE_ID
FROM
    {{ ref('intermediate_hrcy_assn_unique') }} AS main
INNER JOIN
    {{ source('Redshift_gdap_Stage','std_sld_menu_itm') }} AS lkp
    ON cast(main.TERRITORY_CODE as STRING) = lkp.TERR_CD
    AND main.SMI_ID = lkp.sld_menu_itm_id