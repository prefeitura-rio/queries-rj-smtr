SELECT 
  data                  AS as_at,
  CASE
    WHEN data = MAX(data) OVER (PARTITION BY id_veiculo) THEN 'TRUE'
    ELSE 'FALSE'
  END                   AS latest_capture, 
  modo                  AS mode,
  id_veiculo            AS vehicle_id,
  ano_fabricacao        AS manufacture_year,
  carroceria            AS body_model,
  data_ultima_vistoria  AS last_inspection_date,
  id_carroceria         AS body_id,
  id_chassi             AS chassis_id,
  id_fabricante_chassi  AS chassis_manufacturer_id,
  id_interno_carroceria AS internal_bodywork_id,
  id_planta             AS plant_id,
  indicador_ar_condicionado AS aircon_indicator,
  indicador_elevador        AS elevator_indicator,
  indicador_usb             AS usb_indicator,
  indicador_wifi            AS wifi_indicator,
  nome_chassi               AS chassis_name,
  permissao                 AS permit_number,
  placa                     AS plate_id,
  quantidade_lotacao_pe     AS passengers_standing,
  quantidade_lotacao_sentado AS passengers_sitting,
  tipo_combustivel          AS fuel_type,
  tipo_veiculo              AS vehicle_type,
  status                    AS status
FROM rj-smtr.veiculo.sppo_licenciamento