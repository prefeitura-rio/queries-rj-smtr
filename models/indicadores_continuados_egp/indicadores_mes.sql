
SELECT 
    ano,
    mes,
    modo,
    indicador_passageiro_pagante_mes AS valor,
    "Indicador de passageiros pagantes por mês" AS indicador
FROM {{ref ('passageiros_pagantes_onibus')}}

UNION ALL

SELECT 
    ano,
    mes,
    modo,
    indicador_passageiro_pagante_mes AS valor, -- depois de indicador
    "Indicador de passageiros pagantes por mês" AS indicador
FROM {{ref ('passageiros_pagantes_brt')}}

UNION ALL

SELECT
    ano,
    mes,
    modo,
    indicador_gratuidade_mes AS valor,
    "Indicador de gratuidade por mês" AS indicador

FROM {{ref ('indicador_gratuidade_onibus')}}

UNION ALL
SELECT 
    ano,
    mes,
    modo,
    indicador_gratuidade_mes AS valor,
    "Indicador de gratuidade por mês" AS indicador
FROM {{ref ('indicador_gratuidade_brt')}}

UNION ALL 

SELECT 
    ano,
    mes,
    NULL AS modo,
    indicador_frota AS valor,
    "Indicador de frota operante por mês" AS indicador

FROM {{ref ('indicador_frota_operante_mes')}}

UNION ALL

SELECT 
    ano,
    mes,
    NULL AS modo,
    indicador_frota AS valor,
    "Indicador de frota operante por mês" AS indicador

FROM {{ref ('indicador_frota_operante_mes')}}

UNION ALL

SELECT
    ano,
    mes,
    NULL AS modo,
    indicador_idade_media AS valor,
    "Indicador de idade média da frota por mês" AS indicador

FROM {{ref ('indicador_idade_media_frota_mes')}}
