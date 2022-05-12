

-- 1. Selecina shapes filtrados e adiciona identificador da empresa que opera a linha do shape
-- with shape_empresa as (
--     select
--         s.* except(linha)
--         REGEXP_REPLACE(linha, "(S|A|N|V|P|R|E|D|B|C|F|G| )", "") as linha
--         e.cod_empresa as id_empresa
--     from
--         {{ ref("aux_shapes_filtrada") }} s
--     inner join
--         {{ var("aux_empresa_linha") }} e
--     on 
--         s.linha = e.linha
-- ),
-- 3. Filtra shapes de servicos circulares planejados (recupera sentido dos shapes separados em ida/volta)
with shape_circular as (
    select distinct
        s.shape_id,
        c.sentido
    from 
        {{ ref("aux_shapes_filtrada") }} s -- shape_empresa s
    inner join (
        select distinct 
            servico,
            data, 
            sentido
        from 
            {{ ref("viagem_planejada") }}
        where
            sentido = "C"
    ) c
    on 
        s.servico = c.servico
        and s.data = c.data
),
-- 4. Filtra shapes de servicos não circulares planejados
shape_nao_circular as (
    select distinct
        s.shape_id,
        c.sentido
    from 
        {{ ref("aux_shapes_filtrada") }} s -- shape_empresa s
    inner join (
        select distinct 
            servico, 
            data,
            sentido
        from 
            {{ ref("viagem_planejada") }}
        where 
            sentido = "I" or sentido = "V"
    ) c
    on 
        s.servico = c.servico
        and s.sentido_shape = c.sentido
        and s.data = c.data
),
-- 5. Junta infos de shapes ciculares e não ciculares
shape_sentido as (
    select 
        * 
    from 
        shape_circular
    union all  (
        select 
            *
        from 
            shape_nao_circular
    )
)
select
    e.*,
    s.sentido,
    '{{ var("projeto_subsidio_sppo_version") }}' as versao_modelo
from 
    {{ ref("aux_shapes_filtrada") }} e -- shape_empresa e
inner join 
    shape_sentido s
on 
    e.shape_id = s.shape_id