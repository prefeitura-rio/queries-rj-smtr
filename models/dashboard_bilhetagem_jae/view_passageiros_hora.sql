SELECT
    p.data,
    p.hora,
    p.modo,
    p.consorcio,
    p.id_servico_jae,
    s.servico,
    s.descricao_servico,
    CONCAT(s.servico, ' - ' ,s.descricao_servico) AS nome_completo_servico,
    s.latitude AS latitude_servico,
    s.longitude AS longitude_servico,
    p.sentido,
    p.tipo_transacao_smtr,
    p.tipo_transacao_detalhe_smtr,
    p.quantidade_passageiros
FROM
    {{ ref("passageiros_hora") }} p
LEFT JOIN
    {{ ref("servicos") }} s
USING(id_servico_jae)