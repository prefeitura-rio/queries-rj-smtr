{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        unique_key=["data", "id_veiculo"],
        incremental_strategy="insert_overwrite",
    )
}}

{%- if execute %}
  {% set licenciamento_date = run_query("SELECT MIN(data) FROM " ~ this ~ " WHERE data >= DATE_ADD(DATE('" ~ var("run_date") ~ "'), INTERVAL 5 DAY)").columns[0].values()[0] %}
{% endif -%}

with
    -- Tabela de licenciamento
    stu as (
        select 
            *
        from
            {{ ref("sppo_licenciamento_stu") }} as t
        where
        {% if var("stu_data_versao") != "" %}
            data = date("{{ var('stu_data_versao') }}")
        {% else %}
            data = DATE("{{ licenciamento_date }}")
        {% endif %}
            and tipo_veiculo not like "%ROD%"
    ),
    stu_rn AS (
        select
            * except (timestamp_captura),
            ROW_NUMBER() OVER (PARTITION BY data, id_veiculo) rn
        from
            stu
    )
select
  * except(rn)
from
  stu_rn
where
  rn = 1

