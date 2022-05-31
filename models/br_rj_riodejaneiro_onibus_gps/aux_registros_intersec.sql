/*Descrição:
Calcula intersecções das posições (posicao_veiculo_geo) registradas com o traçado (shape) da linha informada.
O cálculo é realizado para todos os itinerários relacionados à uma dada linha, unicamente identificados pelo
"trip_id".
1. Geramos uma tabela com apenas uma coluna 'faixa_horaria', utilizada para agregar os registros a cada 5 minutos
2. Junção (Join) da tabela de registros_filtrada com as faixas horarias criada em 1
3. Cálculo das intersecções. Como descrito acima, contamos quantas vezes as posições informadas nos registros estiveram
a pelo menos 100 metros do traçado planejado.
4. Calculamos um 'status' relativo ao estado da viagem sendo realizada na faixa horária considerada. Para tal, usamos a
mesma metodologia das intersecções, mas definindo o buffer em torno dos pontos iniciais e finais de cada traçado. Assim,
conseguimos identificar começo, meio ou fim da viagem.
5. Definimos em outra tabela uma 'data_versao_efetiva', esse passo serve tanto para definir qual versão do SIGMOB 
utilizaremos em caso de falha na captura do SIGMOB, quanto para definir qual versão será utilizada para o cálculo 
retroativo do histórico de registros que temos.

*/

WITH
registros as (
	SELECT *
	FROM {{	 registros_filtrada }}
    WHERE
		data between DATE({{ date_range_start }}) and DATE({{ date_range_end }})
    AND
		timestamp_gps > {{ date_range_start }} and timestamp_gps <= {{ date_range_end }}
),
times AS ( 
	-- 1. Geração das faixas horárias
	SELECT
		faixa_horaria
	FROM (
		SELECT
			CAST(MIN(data) AS TIMESTAMP) min_date,
			TIMESTAMP_ADD(CAST(MAX(data) AS TIMESTAMP),INTERVAL 1 day) max_date
		FROM registros) r
	JOIN
		UNNEST (GENERATE_TIMESTAMP_ARRAY(r.min_date,
			r.max_date,
		INTERVAL {{ faixa_horaria_minutos }} minute) ) faixa_horaria ),
faixas AS ( 
	-- 2. Join da registros_filtrada com as faixas horárias geradas acima
	SELECT
		id_veiculo,
		linha,
		timestamp_captura,
		faixa_horaria,
		longitude,
		latitude,
		posicao_veiculo_geo,
		DATA,
		hora
	FROM
		times t
	JOIN
		registros r
	ON
		(r.timestamp_captura BETWEEN datetime(faixa_horaria)
		AND datetime(TIMESTAMP_ADD(faixa_horaria, INTERVAL {{ faixa_horaria_minutos }} minute))) ),
intersects AS ( 
	SELECT
		id_veiculo,
		f.linha AS linha_gps,
		s.linha_gtfs,
		shape_distance AS distancia,
		data,
		hora,
		faixa_horaria,
		s.shape_id AS trip_id,
		MIN(timestamp_captura) AS timestamp_inicio,
		COUNT(timestamp_captura) AS total_capturas,
		-- 3. Contagem do número de intersecções a cada faixa_horaria
		COUNT(CASE
			WHEN st_dwithin(posicao_veiculo_geo, shape, {{ tamanho_buffer_metros}}) THEN 1
		END
		) n_intersec,
		-- 4. Identificação do estado da viagem a cada faixa horária
		CASE
		WHEN COUNT(CASE
			WHEN st_dwithin(start_pt,
			posicao_veiculo_geo,
			{{ buffer_inicio_fim_metros }}) IS TRUE THEN 1
		END
		)>=1 THEN 'start'
		WHEN COUNT(CASE
			WHEN st_dwithin(end_pt,
			posicao_veiculo_geo,
			{{ buffer_inicio_fim_metros }}) IS TRUE THEN 1
		END
		)>=1 THEN 'end'
		ELSE
		'middle'
	END
		AS status
	-- 5. Junção com data_versao_efetiva
	FROM (
		SELECT 
			t1.*,
			t2.data_versao_efetiva_shapes as data_versao_efetiva
		FROM faixas t1
		JOIN  {{ data_versao_efetiva }} t2
		ON t1.data = t2.data) f
	JOIN
		{{ shapes }} s
	ON
		s.data_versao = f.data_versao_efetiva
		AND f.linha = s.linha_gtfs
	GROUP BY
		id_veiculo,
		faixa_horaria,
		linha_gps,
		linha_gtfs,
		trip_id,
		data,
		hora,
		distancia )
SELECT
  *,
  STRUCT({{ maestro_sha }} AS versao_maestro,
    {{ maestro_bq_sha }} AS versao_maestro_bq) versao
FROM
  intersects
WHERE
  n_intersec > 0