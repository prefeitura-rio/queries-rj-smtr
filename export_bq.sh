bq extract 
--destination_format CSV
--compression GZIP
--field_delimiter ","
--print_header [true, false] 
rj-smtr-dev:projeto_subsidio_sppo.registros_status_viagem$
gs://[BUCKET]/part_col=[DATE]/[FILENAME]-*.[csv, json, avro]