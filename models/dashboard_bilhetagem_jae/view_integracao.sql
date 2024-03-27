WITH dados_filtrados AS (
    SELECT 
        i.data,
        i.hora,
        i.id_integracao,
        i.sequencia_integracao,
        i.modo,
        s.descricao_servico,
        i.consorcio,
        i.datetime_transacao
    FROM 
        {{ ref("integracao") }} i
    LEFT JOIN
        {{ ref("servicos") }} s
    USING(id_servico_jae)
    WHERE
        data >= "2024-02-24"
        AND servico NOT IN ("888888", "999999")
        AND id_operadora != "2"
)
SELECT
    a.data,
    a.hora,
    a.id_integracao,
    a.sequencia_integracao AS perna_origem,
    a.modo AS modo_origem,
    CONCAT(a.modo, '(', a.sequencia_integracao, ')') AS modo_origem_perna,
    a.consorcio AS consorcio_origem,
    a.descricao_servico AS descricao_servico_origem,
    CONCAT(a.descricao_servico, '(', a.sequencia_integracao, ')') AS descricao_servico_origem_perna,
    b.sequencia_integracao AS perna_destino,
    b.modo AS modo_destino,
    CONCAT(b.modo, '(', b.sequencia_integracao, ')') AS modo_destino_perna,
    b.consorcio AS consorcio_destino,
    b.descricao_servico AS descricao_servico_destino,
    CONCAT(b.descricao_servico, '(', b.sequencia_integracao, ')') AS descricao_servico_destino_perna,
    TIMESTAMP_DIFF(b.datetime_transacao, a.datetime_transacao, MINUTE) AS tempo_integracao_minutos
FROM
    dados_filtrados a
JOIN
    dados_filtrados b
ON
    a.id_integracao = b.id_integracao
    AND a.sequencia_integracao = b.sequencia_integracao - 1

