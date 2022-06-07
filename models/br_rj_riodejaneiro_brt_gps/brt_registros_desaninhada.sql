SELECT 
data,
hora,
id_veiculo,
timestamp_gps,
timestamp_captura,
SAFE_CAST(json_value(content,"$.latitude") AS FLOAT64) latitude,
SAFE_CAST(json_value(content,"$.longitude") AS FLOAT64) longitude,
json_value(content,"$.servico") servico,
json_value(content,"$.sentido") sentido,
SAFE_CAST(json_value(content,"$.velocidade") AS INT64) velocidade,
from {{ ref('brt_registros') }} as t