{% macro get_last_feed_start_date(data_versao_gtfs) %}
  {{ return(run_query("SELECT MAX(feed_start_date) FROM " ~ ref('feed_info_gtfs') ~ " WHERE feed_start_date < " ~ "'" ~ data_versao_gtfs ~ "'").columns[0].values()[0]) }}
{% endmacro %}