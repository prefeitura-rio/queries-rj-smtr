WITH t AS (
SELECT 
    data_versao,
    case when json_value(content, '$.IDPropriedadeParada') is null then false else true end flag_vistoriada,
    json_value(content, '$.AP') as AP,
    json_value(content, '$.RA') as RA,
    json_value(content, '$.Bairro') as Bairro
FROM {{ stops }}
where json_value(content, '$.PontoExistente') = 'SIM'
and json_value(content, '$.idModalSmtr') = '22')
select data_versao, AP, Bairro,
    sum(case when flag_vistoriada then 1 else 0 end) n_vistoridas,
    count(*) n_pontos
from t
group by data_versao, AP, Bairro
order by 1,2,3 