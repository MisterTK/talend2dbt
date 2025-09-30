{{ config(
    materialized='table',
    tags = ['s89_itm_hrcy_grandmaster','s89_200_itm_hrcy_assn','s89_200_itm_hrcy_assn_intermediate']
) }}

SELECT
    main.TERRITORY_CODE,
    main.SMI_ID,
    main.GLOBAL_MENU_IDENTIFIER,
    main.HIERARCHY_NAME,
    main.HIERARCHY_NODE_ID,
    main.EFFECTIVE_DATE,
    main.DW_FILE_ID,
    'SMI ID is rejected please check VISTA_87 reject folder for rejection reason' AS errorMessage
FROM
    {{ ref('intermediate_hrcy_assn_unique') }} AS main
LEFT JOIN
    {{ source('Redshift_gdap_Stage','std_sld_menu_itm') }} AS lkp
    ON cast(main.TERRITORY_CODE as STRING) = lkp.TERR_CD
    AND main.SMI_ID = lkp.sld_menu_itm_id
WHERE
    lkp.TERR_CD IS NULL