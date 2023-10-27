### --- Reprocessamento para serviços antes de 16/11/2022 --- ###

# Este script executa os modelos que contêm as tabelas gps_sppo e viagem_completa com os dados que 
# serão usados para reprocessamentos do serviço antes de 16/11/2022.



from utils import run_dbt_model, dbt_seed
from datetime import datetime, timedelta
import pandas as pd

dados = pd.read_csv('./data/reprocessar.csv')


# Aumentar os datetime_partida (para menos) e datetime_chegada (para mais) em metade da
# duração da viagem, para me certificar que estou pegando os sinais de start e end.
dados['datetime_partida_amostra'] = pd.to_datetime(dados['datetime_partida_amostra'])
dados['datetime_chegada_amostra'] = pd.to_datetime(dados['datetime_chegada_amostra'])
dados['diferenca_minutos'] = (dados['datetime_chegada_amostra'] - dados['datetime_partida_amostra']).dt.total_seconds() / 60
dados['diferenca_minutos'] = dados['diferenca_minutos'] * 1.5
dados['datetime_chegada_amostra'] = dados['datetime_chegada_amostra'] + pd.to_timedelta(dados['diferenca_minutos'], unit='m')
dados['datetime_partida_amostra'] = dados['datetime_partida_amostra'] - pd.to_timedelta(dados['diferenca_minutos'], unit='m')
dados = dados.drop(columns='diferenca_minutos')

dates = dados['data'].drop_duplicates().astype(str).tolist()


# iterar sobre as datas para rodar o modelo de GPS

for date in dates: 
    
    dados_filtrados = dados[dados['data'] == date] 
    dados_filtrados = dados_filtrados[['data','id_veiculo_amostra','datetime_partida_amostra','datetime_chegada_amostra','servico_amostra']]

    dados_filtrados.to_csv('./data/seed_viagens.csv', index=False)
    dbt_seed() # preciso rodar o dbt seed a cada iteração após a linha acima
    
    date = pd.to_datetime(date).date()
    date = date + timedelta(days=1)
    date = date.strftime('%Y-%m-%d')
    
    #  1 - Rodar o modelo gps_sppo
    
    #  Dataset com dados de GPS reprocessados `rj-smtr-dev.br_rj_riodejaneiro_veiculos_recursos_reprocessado`
    print('Executando modelo gps_sppo para o dia ' + date)

    run_dbt_model(
        dataset_id="br_rj_riodejaneiro_veiculos",
        table_id="gps_sppo",
        upstream=True,
        exclude="+shapes_geom", 
        _vars={"date_range_start": f"{date} 00:00:00", 
               "date_range_end": f"{date} 23:59:59",
               "reprocessed_service": True,             
               "version": ""},
    )
    
    # 2 - Rodar o modelo de viagens completas e conformidade
    # Dados disponíveis em: `rj-smtr-dev.projeto_subsidio_sppo_recursos_reprocessado.viagem_completa` 
    print('Executando modelos de viagens para o dia ' + date)
        
    run_dbt_model(
        dataset_id="projeto_subsidio_sppo",
        table_id="viagem_completa",
        upstream=True,
        exclude="+gps_sppo",
        _vars={"run_date": date,
                "version": ""}, 
    )
    
print('Reprocessamento finalizado.')