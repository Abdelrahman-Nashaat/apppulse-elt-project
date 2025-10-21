{% macro duckdb_mysql_connect() %}
  INSTALL mysql;
  LOAD mysql;
  ATTACH 'host=localhost user=appuser passwd=apppass db=apppulse' AS apppulse_mysql (TYPE MYSQL, READ_ONLY);
{% endmacro %}
