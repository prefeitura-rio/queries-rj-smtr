{{
  config(
    alias='percentual_rateio_integracao',
  )
}}

{{
    open_staging_table(
        pk_columns={
            'id': 'STRING'
        },
        content_columns={
            'dt_fim_validade': 'STRING',
            'dt_inclusao': {'type': 'DATETIME', 'format': '%Y-%m-%dT%H:%M:%S%Ez'},
            'dt_inicio_validade': {'type': 'DATE', 'format': '%Y-%m-%d'},
            'id_tipo_modal_integracao_t1': 'INTEGER',
            'id_tipo_modal_integracao_t2': 'INTEGER',
            'id_tipo_modal_integracao_t3': 'INTEGER',
            'id_tipo_modal_integracao_t4': 'INTEGER',
            'id_tipo_modal_origem': 'INTEGER',
            'perc_rateio_integracao_t1': 'FLOAT64',
            'perc_rateio_integracao_t2': 'FLOAT64',
            'perc_rateio_integracao_t3': 'FLOAT64',
            'perc_rateio_integracao_t4': 'FLOAT64',
            'perc_rateio_origem': 'FLOAT64'
        },
        source_dataset_id='br_rj_riodejaneiro_bilhetagem_staging',
        source_table_id='percentual_rateio_integracao'
    )
}}