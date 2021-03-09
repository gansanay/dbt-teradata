{% macro teradata__create_schema(relation) -%}
  {% call statement('create_schema') -%}
    select 1
  {%- endcall %}
  {{ log('create_schema macro (' + relation.render() + ') not implemented yet for adapter ' + adapter.type(), info=True) }}
{% endmacro %}

{% macro teradata__check_schema_exists(information_schema, schema) -%}
  {% call statement('check_schema_exists') -%}
    select 1
  {%- endcall %}
  {{ log('check_schema_exists macro not implemented yet for adapter ' + adapter.type(), info=True) }}
{% endmacro %}

{% macro teradata__drop_schema(relation) -%}
  {% call statement('drop_schema') -%}
    select 1
  {%- endcall %}
  {{ log('drop_schema macro not implemented yet for adapter ' + adapter.type(), info=True) }}
{% endmacro %}

{% macro teradata__list_schemas(database) -%}
  {% call statement('list_schemas') -%}
    select 1
  {%- endcall %}  
  {{ log('list_schemas macro not implemented yet for adapter ' + adapter.type(), info=True) }}
{% endmacro %}

{% macro teradata__drop_relation(relation) -%}
  {% call statement('drop_relation', auto_begin=False) -%}
    SELECT 1 FROM	DBC.TablesV
    WHERE DataBaseName = '{{ relation.database }}' 
    AND TableName = '{{ relation.identifier }}';
    .IF activitycount = 0 THEN GOTO ok
    DROP {{ relation.type }} {{ relation }};
    .LABEL ok
  {%- endcall %}
{% endmacro %}

{% macro teradata__get_columns_in_relation(relation) -%}
    {% call statement('get_columns_in_relation', fetch_result=True) %}
        select 
            ColumnName
            , ColumnType
            , ColumnLength
        from 
            DBC.ColumnsV
        where 
            DataBaseName    = '{{ relation.database }}'
            and TableName   = '{{ relation.identifier }}'
    {% endcall %}

    {% set table = load_result('get_columns_in_relation').table %}
    {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}

{% macro teradata__list_relations_without_caching(information_schema, schema) %}
    {% call statement('list_relations_without_caching', fetch_result=True) %}
        select
            DataBaseName database_name
            , TableName table_name
            , database_name
            , case 
                when TableKind = 'O'
                    then 'table'
                when TableKind = 'V'
                    then 'view'
                else TableKind
            end table_type
        from 
            DBC.TablesV
        where 
            DataBaseName = '{{ information_schema.database }}'
    {% endcall %}

    {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}

{% macro teradata__alter_column_type(relation, column_name, new_column_type) -%}
  {#
    1. Create a new column (w/ temp name and correct type)
    2. Copy data over to it
    3. Drop the existing column (cascade!)
    4. Rename the new column to existing column
  #}
  {%- set tmp_column = column_name + "__dbt_alter" -%}

  {% call statement('alter_column_type') %}
    alter table {{ relation }} add ({{ tmp_column }} {{ new_column_type }}) no auto compress;
    update {{ relation }} set {{ tmp_column }} = {{ column_name }};
    alter table {{ relation }} drop {{ column_name }};
    alter table {{ relation }} rename {{ tmp_column }} to {{ column_name }}
  {% endcall %}

{% endmacro %}

{% macro teradata__rename_relation(from_relation, to_relation) -%}
  {% set target_name = adapter.quote_as_configured(to_relation.identifier, 'identifier') %}
  {% call statement('rename_relation') -%}
    rename table {{ from_relation }} to {{ to_relation }}
  {%- endcall %}
{% endmacro %}

{% macro teradata__truncate_relation(relation) -%}
  {% call statement('truncate_relation') -%}
    delete from {{ relation }} all
  {%- endcall %}
{% endmacro %}