CREATE OR REPLACE TABLE keepcoding.ivr_summary AS
WITH tipo_documento_unico 
  AS (SELECT detail.calls_ivr_id
           , MAX(document_type) AS document_type
    FROM keepcoding.ivr_detail AS detail
    WHERE document_type<>'NULL' AND document_type<>'DESCONOCIDO'
    GROUP BY detail.calls_ivr_id)
    ,documento_unico 
  AS (SELECT detail.calls_ivr_id
           , MAX(document_identification) as document_identification
    FROM keepcoding.ivr_detail AS detail
    WHERE document_identification<>'NULL'
    GROUP BY detail.calls_ivr_id)
    ,customer_phone_unico
  AS (SELECT detail.calls_ivr_id
           , customer_phone
      FROM keepcoding.ivr_detail AS detail
      WHERE customer_phone<>'NULL'
      GROUP BY detail.calls_ivr_id
             , customer_phone)
    ,billing_account_unico
  AS (SELECT detail.calls_ivr_id
           , MAX(billing_account_id) AS billing_account_id
      FROM keepcoding.ivr_detail AS detail
      WHERE billing_account_id <> 'NULL'
      GROUP BY detail.calls_ivr_id)
    ,averia_masiva_flag
  AS (SELECT detail.calls_ivr_id
     , MAX(IF(module_name = 'AVERIA_MASIVA', 1, 0)) AS averia_masiva_flag 
      FROM keepcoding.ivr_detail AS detail
      GROUP BY detail.calls_ivr_id)
    ,identification_phone_step_flag
  AS (SELECT detail.calls_ivr_id
     , MAX(IF(step_name = 'CUSTOMERINFOBYPHONE.TX' AND step_description_error = 'NULL', 1, 0)) AS identification_phone_step_flag 
      FROM keepcoding.ivr_detail AS detail
      GROUP BY detail.calls_ivr_id)
    , identification_dni_step_flag
  AS (SELECT detail.calls_ivr_id
     , MAX(IF(step_name = 'CUSTOMERINFOBYDNI.TX' AND step_description_error = 'NULL', 1, 0)) AS identification_dni_step_flag 
      FROM keepcoding.ivr_detail AS detail
      GROUP BY detail.calls_ivr_id)
    , repeated_phone_24H_flag
  AS (SELECT detail.calls_ivr_id
     , detail.calls_phone_number
     , MAX(IF(DATE_DIFF(detail_aux.calls_start_date, detail.calls_start_date, SECOND) BETWEEN -86400 AND -1, 1, 0)) as difference_minus_flag --tiempo en segundos, pueden haber llamado SEGUNDOS antes
      FROM keepcoding.ivr_detail AS detail -- 86400 es el número de segundos en un día
      JOIN keepcoding.ivr_detail AS detail_aux ON detail.calls_phone_number = detail_aux.calls_phone_number
      GROUP BY detail.calls_ivr_id
             , detail.calls_phone_number)
    , cause_recall_phone_24H_flag
  AS (SELECT detail.calls_ivr_id
     , detail.calls_phone_number
     , MAX(IF(DATE_DIFF(detail_aux.calls_start_date, detail.calls_start_date, SECOND) BETWEEN 1 AND 86400, 1, 0)) as difference_plus_flag --tiempo en segundos, pueden haber llamado SEGUNDOS después
      FROM keepcoding.ivr_detail AS detail -- 86400 es el número de segundos en un día
      JOIN keepcoding.ivr_detail AS detail_aux ON detail.calls_phone_number = detail_aux.calls_phone_number
      GROUP BY detail.calls_ivr_id
             , detail.calls_phone_number)

SELECT  detail.calls_ivr_id AS ivr_id
      , IF(detail.calls_phone_number = 'NULL', 'NO CONSTA', detail.calls_phone_number) AS phone_number
      , IF(detail.calls_ivr_result = 'NULL', 'NO CONSTA', detail.calls_ivr_result) AS ivr_result
      , CASE WHEN STARTS_WITH(calls.vdn_label, 'ATC') THEN 'FRONT'
             WHEN STARTS_WITH(calls.vdn_label, 'TECH') THEN 'TECH'
             WHEN STARTS_WITH(calls.vdn_label, 'ABSORPTION') THEN 'ABSORPTION'
             ELSE 'RESTO'
        END AS vdn_aggregation
      , detail.calls_start_date AS start_date
      , detail.calls_end_date AS end_date
      , detail.calls_total_duration AS total_duration
      , IF(detail.calls_customer_segment = 'NULL', 'NO CONSTA', detail.calls_customer_segment) AS customer_segment
      , IF(detail.calls_ivr_language = 'NULL', 'NO CONSTA', detail.calls_ivr_language) AS ivr_languaje
      , detail.calls_steps_module AS steps_module
      , IF(detail.calls_module_aggregation IS NULL, 'NO CONSTA', detail.calls_module_aggregation) AS module_aggregation
      , IF(tipo_documento_unico.document_type IS NULL, 'NO CONSTA', tipo_documento_unico.document_type) AS document_type
      , IF(documento_unico.document_identification IS NULL, 'NO CONSTA', documento_unico.document_identification) AS document_identification 
      , IF(customer_phone_unico.customer_phone IS NULL, 'NO CONSTA', customer_phone_unico.customer_phone) AS customer_phone
      , IF(billing_account_unico.billing_account_id IS NULL, 'NO CONSTA', billing_account_unico.billing_account_id) AS billing_account_id 
      , averia_masiva_flag.averia_masiva_flag AS masiva_lg
      , identification_phone_step_flag.identification_phone_step_flag AS info_by_phone_lg
      , identification_dni_step_flag.identification_dni_step_flag AS info_by_dni_lg
      , repeated_phone_24H_flag.difference_minus_flag AS repeated_phone_24H
      , cause_recall_phone_24H_flag.difference_plus_flag AS cause_recall_phone_24H
  FROM keepcoding.ivr_detail AS detail 
  LEFT JOIN keepcoding.ivr_calls AS calls ON detail.calls_ivr_id = calls.ivr_id
  LEFT JOIN tipo_documento_unico ON tipo_documento_unico.calls_ivr_id = calls.ivr_id
  LEFT JOIN documento_unico ON documento_unico.calls_ivr_id = calls.ivr_id
  LEFT JOIN customer_phone_unico ON customer_phone_unico.calls_ivr_id = calls.ivr_id
  LEFT JOIN billing_account_unico ON billing_account_unico.calls_ivr_id = calls.ivr_id
  LEFT JOIN averia_masiva_flag ON averia_masiva_flag.calls_ivr_id = calls.ivr_id
  LEFT JOIN identification_phone_step_flag ON identification_phone_step_flag.calls_ivr_id = calls.ivr_id
  LEFT JOIN identification_dni_step_flag on identification_dni_step_flag.calls_ivr_id = calls.ivr_id
  LEFT JOIN repeated_phone_24H_flag ON repeated_phone_24H_flag.calls_ivr_id = calls.ivr_id
  LEFT JOIN cause_recall_phone_24H_flag ON cause_recall_phone_24H_flag.calls_ivr_id = calls.ivr_id
  GROUP BY ivr_id
          ,phone_number
          ,ivr_result
          ,vdn_aggregation
          ,start_date
          ,end_date
          ,total_duration
          ,customer_segment
          ,ivr_languaje
          ,steps_module
          ,module_aggregation
          ,document_type
          ,document_identification
          ,customer_phone
          ,billing_account_id 
          ,masiva_lg
          ,info_by_phone_lg
          ,info_by_dni_lg
          ,repeated_phone_24H
          ,cause_recall_phone_24H;
 