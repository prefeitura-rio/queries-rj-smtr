WITH t AS (
    SELECT SAFE_CAST(servico AS STRING) servico,
        REPLACE(content, "None", "") content,
        --    SAFE_CAST(data_versao AS DATE) data_versao
    FROM { { var('quadro_staging') } }
)
SELECT agency_id,
    JSON_VALUE(content, "$.vista") vista,
    JSON_VALUE(content, "$.consorcio") consorcio,
    JSON_VALUE(content, "$.horario_inicial") horario_inicial,
    JSON_VALUE(content, "$.horario_fim") horario_fim,
    JSON_VALUE(content, "$.extensao_de_ida") extensao_de_ida,
    JSON_VALUE(content, "$.extensao_de_volta") extensao_de_volta,
    JSON_VALUE(content, "$.partidas_ida_dia_util") partidas_ida_dia_util,
    JSON_VALUE(content, "$.partidas_volta_dia_util") partidas_volta_dia_util,
    JSON_VALUE(content, "$.viagens_dia_util") viagens_dia_util,
    JSON_VALUE(content, "$.quilometragem_dia_util") quilometragem_dia_util,
    JSON_VALUE(content, "$.partidas_ida_sabado") partidas_ida_sabado,
    JSON_VALUE(content, "$.partidas_volta_sabado") partidas_volta_sabado,
    JSON_VALUE(content, "$.viagens_sabado") viagens_sabado,
    JSON_VALUE(content, "$.quilometragem_sabado") quilometragem_sabado,
    JSON_VALUE(content, "$.partidas_ida_domingo") partidas_ida_domingo,
    JSON_VALUE(content, "$.partidas_volta_domingo") partidas_volta_domingo,
    JSON_VALUE(content, "$.viagens_domingo") viagens_domingo,
    JSON_VALUE(content, "$.quilometragem_domingo") quilometragem_domingo,
    JSON_VALUE(content, "$.partida_ida_ponto_facultativo") partida_ida_ponto_facultativo,
    JSON_VALUE(content, "$.partida_volta_ponto_facultativo") partida_volta_ponto_facultativo,
    JSON_VALUE(content, "$.viagem_ponto_facultativo") viagem_ponto_facultativo,
    JSON_VALUE(content, "$.quilometragem_ponto_facultativo") quilometragem_ponto_facultativo,

    --   DATE(data_versao) data_versao
FROM t