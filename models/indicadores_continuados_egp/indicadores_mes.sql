
SELECT 
    ano,
    mes,
    modo,
    "Indicador de passageiros pagantes por mês" AS nome_indicador,
    quantidade_passageiro_pagante_mes AS valor
FROM {{ref ("passageiro_pagante_onibus")}}

UNION ALL

SELECT 
    ano,
    mes,
    modo,
    "Indicador de passageiros pagantes por mês" AS nome_indicador,
    quantidade_passageiro_pagante_mes AS valor,
FROM {{ref ("passageiro_pagante_brt")}}

UNION ALL

SELECT
    ano,
    mes,
    modo,
    "Indicador de gratuidade por mês" AS nome_indicador,
    quantidade_passageiro_gratuidade_mes AS valor,
FROM {{ref ("passageiro_gratuidade_onibus")}}

UNION ALL
SELECT 
    ano,
    mes,
    modo,
    "Indicador de gratuidade por mês" AS nome_indicador,
    quantidade_passageiro_gratuidade_mes AS valor,
FROM {{ref ("passageiro_gratuidade_brt")}}

UNION ALL 

SELECT 
    ano,
    mes,
    NULL AS modo,
    "Indicador de frota operante por mês" AS nome_indicador,
    quantidade_veiculos_mes AS valor,
FROM {{ref ("frota_operante_onibus")}}

UNION ALL

SELECT
    ano,
    mes,
    NULL AS modo,
    "Indicador de idade média da frota por mês" AS nome_indicador,
    idade_media_veiculos_mes AS valor,
FROM {{ref ("idade_media_frota")}}
