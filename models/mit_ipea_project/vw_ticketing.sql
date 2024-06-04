-- ticketing view, ticketing data cleaned for:
-- - card types that do not unique identify users (these include cash transactions)
-- - pre-identified card ids with extreme values
CREATE OR REPLACE VIEW `rj-smtr-dev.mit_ipea_project.vw_ticketing` AS

WITH raw_ticketing AS (SELECT data                                                               AS as_at,
                              EXTRACT(time from datetime)                                        AS origin_time,
                              id_cartao                                                          AS card_id,
                              tipo_cartao                                                        AS card_type,
                              ROW_NUMBER() OVER (PARTITION BY id_cartao, data ORDER BY datetime) AS daily_trip_id,
                              id_empresa                                                         AS company_id,
                              id_veiculo                                                         AS vehicle_id

                       FROM `rj-smtr.br_rj_riodejaneiro_onibus_tg.transacao`
                       WHERE tipo_cartao NOT IN (SELECT chave
                                                 FROM `rj-smtr.br_rj_riodejaneiro_onibus_tg.dicionario`
                                                 -- Card types to drop:
                                                 WHERE VALOR IN (
                                                                 'RIOONIBUS - BOTOEIRA',
                                                                 'RIOONIBUS - ROD. CIDADAO RIO ONIBUS',
                                                                 'RIOONIBUS - RODOVIARIO RIO ONIBUS'
                                                     )))

SELECT *,
       CASE
           WHEN MAX(daily_trip_id) OVER (PARTITION BY card_id, as_at) = 1
               THEN 'Only transaction'
           WHEN MAX(daily_trip_id) OVER (PARTITION BY card_id, as_at) = daily_trip_id
               THEN 'Last transaction'
           WHEN MIN(daily_trip_id) OVER (PARTITION BY card_id, as_at) = daily_trip_id
               THEN 'First transaction'
           ELSE
               'Intermediate transaction'
           END
           AS daily_trip_stage

FROM raw_ticketing
# These card_ids have an average of >400 transactions per day
WHERE card_id NOT IN ('2e6e15a38c6fe8b624fca13be00a737947a8096fd5620795696b5b63cd7feea4',
               '9af15b336e6a9619928537df30b2e6a2376569fcf9d7e773eccede65606529a0')

