{% macro glma_prmo_itm_load() %}
{% do log("Execution of glma_prmo_itm_load started", info=True) %}
-- Clean CDC table
DELETE FROM {{ source('rmdw_utility', 'prmo_itm_cdc') }}
WHERE 1=1;

-- Insert changes into CDC
INSERT INTO {{ source('rmdw_utility', 'prmo_itm_cdc') }}
SELECT
    tmp.prmo_id,
    tmp.sld_menu_itm_id,
    tmp.terr_cd,
    tmp.prmo_itm_strt_dt,
    tmp.prmo_itm_end_dt,
    tmp.flag,
    tmp.itm_prmo_ds,
    tmp.load_dw_audt_ts,
    tmp.updt_dw_audt_ts,
    tmp.dw_file_id
FROM (
    -- Deleted records
    SELECT
        c.prmo_id,
        c.sld_menu_itm_id,
        c.terr_cd,
        c.prmo_itm_strt_dt,
        c.prmo_itm_end_dt,
        c.load_dw_audt_ts,
        c.updt_dw_audt_ts,
        c.dw_file_id,
        'DL' AS flag,
        c.itm_prmo_ds
    FROM {{ source('rmdw_tables', 'prmo_itm') }} AS c
    INNER JOIN (
        SELECT a.prmo_id, a.sld_menu_itm_id, a.terr_cd, a.prmo_itm_strt_dt
        FROM {{ source('rmdw_tables', 'prmo_itm') }} AS a
        JOIN {{ source('rmdw_tables', 'prmo') }} AS b
            ON a.prmo_id = b.prmo_id
        WHERE b.prmo_soft_del_fl = 0
          AND a.terr_cd IN (
              SELECT DISTINCT terr_cd FROM {{ source('rmdw_utility', 'prmo_itm') }}
          )
        EXCEPT DISTINCT
        SELECT prmo_id, sld_menu_itm_id, terr_cd, prmo_itm_strt_dt
        FROM {{ source('rmdw_utility', 'prmo_itm') }}
    ) AS d
    ON c.prmo_id = d.prmo_id
    AND c.sld_menu_itm_id = d.sld_menu_itm_id
    AND c.terr_cd = d.terr_cd
    AND c.prmo_itm_strt_dt = d.prmo_itm_strt_dt

    UNION ALL

    
    SELECT
        c.prmo_id,
        c.sld_menu_itm_id,
        c.terr_cd,
        c.prmo_itm_strt_dt,
        c.prmo_itm_end_dt,
        DATETIME(CURRENT_TIMESTAMP()) AS load_dw_audt_ts,
        DATETIME(CURRENT_TIMESTAMP()) AS updt_dw_audt_ts,
        c.dw_file_id,
        'UI' AS flag,
        c.itm_prmo_ds
    FROM {{ source('rmdw_utility', 'prmo_itm') }} AS c
    INNER JOIN (
        SELECT prmo_id, sld_menu_itm_id, terr_cd, prmo_itm_strt_dt, prmo_itm_end_dt
        FROM {{ source('rmdw_utility', 'prmo_itm') }}
        EXCEPT DISTINCT
        SELECT a.prmo_id, a.sld_menu_itm_id, a.terr_cd, a.prmo_itm_strt_dt, a.prmo_itm_end_dt
        FROM {{ source('rmdw_tables', 'prmo_itm') }} AS a
        JOIN {{ source('rmdw_tables', 'prmo') }} AS b
            ON a.prmo_id = b.prmo_id
        WHERE b.prmo_soft_del_fl = 0
          AND a.terr_cd IN (
              SELECT DISTINCT terr_cd FROM {{ source('rmdw_utility', 'prmo_itm') }}
          )
    ) AS d
    ON c.prmo_id = d.prmo_id
    AND c.sld_menu_itm_id = d.sld_menu_itm_id
    AND c.terr_cd = d.terr_cd
    AND c.prmo_itm_strt_dt = d.prmo_itm_strt_dt
    AND c.prmo_itm_end_dt = d.prmo_itm_end_dt
) AS tmp;


DELETE FROM {{ source('rmdw_tables', 'prmo_itm') }}
WHERE EXISTS (
    SELECT 1
    FROM {{ source('rmdw_utility', 'prmo_itm_cdc') }} AS src
    WHERE
        {{ source('rmdw_tables', 'prmo_itm') }}.prmo_id = src.prmo_id
        AND {{ source('rmdw_tables', 'prmo_itm') }}.sld_menu_itm_id = src.sld_menu_itm_id
        AND {{ source('rmdw_tables', 'prmo_itm') }}.terr_cd = src.terr_cd
        AND {{ source('rmdw_tables', 'prmo_itm') }}.prmo_itm_strt_dt = src.prmo_itm_strt_dt
); -- Added semicolon

-- Insert updated records
INSERT INTO {{ source('rmdw_tables', 'prmo_itm') }}
SELECT
    prmo_id,
    sld_menu_itm_id,
    terr_cd,
    prmo_itm_strt_dt,
    prmo_itm_end_dt,
    itm_prmo_ds,
    load_dw_audt_ts,
    updt_dw_audt_ts,
    dw_file_id
FROM {{ source('rmdw_utility', 'prmo_itm_cdc') }}
WHERE flag = 'UI';

-- Clear utility staging table
DELETE FROM {{ source('rmdw_utility', 'prmo_itm') }}
WHERE 1=1;

{% do log("Execution of glma_prmo_itm_load completed", info=True) %}

{% endmacro %}
