{% 
    macro open_staging_table(
        pk_columns,
        content_columns,
        source_dataset_id,
        source_table_id,
        rn_partition_by_clause = none,
        rn_column_name = "rn",
        additional_filter = none
    )
%}
    WITH aberto AS (    
        SELECT
            data
            {% for column_name, type_info in pk_columns.items() %}
                {% if type_info is string %}
                    ,SAFE_CAST({{ column_name }} AS {{ type_info }}) AS {{ column_name | lower }}
                {% else %}
                    ,{{ type_info.type }}(PARSE_TIMESTAMP('{{ type_info.format }}', {{ column_name }}), {{ type_info.timezone|default ("'America/Sao_Paulo'") }}) AS {{ column_name | lower }}
                {% endif %}
            {% endfor %}
            ,timestamp_captura
            {% for column_name, type_info in content_columns.items() %}
                {% if type_info is string %}
                    ,SAFE_CAST(JSON_VALUE(content, '$.{{ column_name }}') AS {{ type_info }}) AS {{ column_name | lower }}
                {% else %}
                    ,{{ type_info.type }}(PARSE_TIMESTAMP('{{ type_info.format }}', SAFE_CAST(JSON_VALUE(content, '$.{{ column_name }}') AS STRING)), {{ type_info.timezone|default ("'America/Sao_Paulo'") }}) AS {{ column_name | lower }}
                {% endif %}
            {% endfor %}
            FROM
                {{ source(source_dataset_id, source_table_id) }}
            {% if additional_filter is not none %}
                WHERE {{ additional_filter }}
            {% endif %}
    ), aberto_rn AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY {{ rn_partition_by_clause if rn_partition_by_clause is not none else pk_columns.keys() | join(", ") ~ ' ORDER BY timestamp_captura DESC' }}
                ) AS {{ rn_column_name }}
        FROM
            aberto
    )
    SELECT
        * EXCEPT({{ rn_column_name }})
    FROM
        aberto_rn
    WHERE
        rn = 1
{%- endmacro %}
