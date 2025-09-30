{{ config(
    materialized='ephemeral',
    database='prj-cp-dagdwstr-dev01',
    schema='rmdw_int',
    alias='dw_audt_stus_typ_ds',
    tags=['gscv_s34_grand_master', 'gscv_s34_100_dcreceipts_processed_files_extract_child','gscv_s34_100_dcreceipts_processed_files_extract_child_intermediate']
) }}

SELECT *
FROM UNNEST([
  STRUCT("0" AS dw_audt_stus_typ_id, "UNKNOWN" AS dw_audt_stus_typ_ds),
  STRUCT("1", "ARRIVED"),
  STRUCT("2", "VALIDATED"),
  STRUCT("3", "READY"),
  STRUCT("4", "PROCESSING"),
  STRUCT("5", "SUCCESSFUL"),
  STRUCT("6", "REJECTED"),
  STRUCT("7", "FAILED"),
  STRUCT("8", "REPROCESS"),
  STRUCT("9", "HOLD - VALIDATED")
])

