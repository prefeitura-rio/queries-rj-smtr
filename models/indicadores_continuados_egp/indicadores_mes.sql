SELECT 
    EXTRACT(YEAR FROM data) AS ano,
    EXTRACT(MONTH FROM data) AS mes,
    SUM(indicador_passageiro_pagante_dia) AS valor,
    "Indicador de passageiros pagantes por mês - Ônibus" AS indicador
FROM {{ref ('passageiros_pagantes_onibus')}}
    GROUP BY ano, mes
 
UNION ALL

SELECT 
    EXTRACT(YEAR FROM data) AS ano,
    EXTRACT(MONTH FROM data) AS mes,
    SUM(indicador_passageiro_pagante_dia) AS valor,
    "Indicador de passageiros pagantes por mês - BRT" AS indicador
FROM {{ref ('passageiros_pagantes_brt')}}
    GROUP BY ano, mes

UNION ALL

SELECT
    EXTRACT(YEAR FROM data) AS ano,
    EXTRACT("
FROM {{ref ('passageiros_pagantes_brt')}}
    GROUP BY ano, mes
UNION ALL
SELECT 
    EXTRACT(YEAR FROM data) AS ano,
    EXTRACT(MONTH FROM data) AS mes,
    SUM(indicador_gratuidade_dia) AS valor
FROM {{ref ('indicador_gratuidade_brt')}}
    GROUP BY ano, mes
UNION ALL 

SELECT 
    EXTRACT(YEAR FROM data) AS ano,
    EXTRACT(MONTH FROM data) AS mes,
    SUM(indicador_gratuidade_dia) AS valor
FROM {{ref ('indicador_gratuidade_onibus')}}
    GROUP BY ano, mes

UNION ALL 

SELECT 
    ano,
    mes,
    indicador_frota AS valor 

FROM {{ref ('indicador_frota_operante_mes')}}

UNION ALL

SELECT 
    ano,
    mes,
    indicador_idade_media AS valor 
FROM {{ref ('indicador_idade_media_frota_mes')}}
