{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        unique_key=["data", "id_veiculo"],
        incremental_strategy="insert_overwrite",
    )
}}

with
    -- Tabela de licenciamento
    stu as (
        select *
        from {{ ref("sppo_licenciamento_stu") }} as t
        {% if var("stu_data_versao") != "" %}
            where data = date("{{ var('stu_data_versao') }}")
        {% else %}
            {% if execute %}
                {% set licenciamento_date = run_query("SELECT MIN(data) FROM " ~ ref("sppo_licenciamento_stu") ~ " WHERE data >= DATE_ADD(DATE('" ~ var("run_date") ~ "'), INTERVAL 5 DAY)").columns[0].values()[0] %}
            {% endif %}

            where data = DATE("{{ licenciamento_date }}")
        {% endif %}
        AND tipo_veiculo NOT LIKE "%ROD%"
    )
select 
    stu.* except (timestamp_captura)
from stu
