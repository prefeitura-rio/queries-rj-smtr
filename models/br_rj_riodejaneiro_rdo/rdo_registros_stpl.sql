{{ 
  config(
      materialized='incremental',
      partition_by={
            "field":"data_transacao",
            "data_type": "date",
            "granularity":"day"
      }
  )
}}

SELECT
    * EXCEPT(rn)
FROM
    (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY data_transacao, linha, operadora ORDER BY data_processamento DESC) AS rn
        FROM
            {{ref('rdo_registros_stpl_view')}}
        {% if is_incremental() %}
            WHERE
                ano = EXTRACT(YEAR FROM DATE("{{var('run_date')}}"))
                AND mes = EXTRACT(MONTH FROM DATE("{{var('run_date')}}"))
                AND dia = EXTRACT(DAY FROM DATE("{{var('run_date')}}"))
        {% endif %}
    )
WHERE
    rn = 1

