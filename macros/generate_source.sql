{% macro get_tables_in_schema(schema_name, database_name=target.database) %}
    {% set sql %}
        SELECT table_name
        FROM iceberg.information_schema.tables
        WHERE lower(table_catalog) = lower('{{ database_name }}')
          AND lower(table_schema) = lower('{{ schema_name }}')
        ORDER BY table_name
    {% endset %}

    {% set results = run_query(sql) %}
    {% if execute %}
        {% set table_list = results.columns[0].values() %}
        {{ return(table_list | sort) }}
    {% endif %}
{% endmacro %}

{% macro generate_source(schema_name, catalog_name, database_name=target.database, generate_columns=False, include_descriptions=False, include_data_types=True, table_pattern='%', exclude='', name=schema_name, table_names=None, include_database=False, include_schema=False, case_sensitive_databases=False, case_sensitive_schemas=False, case_sensitive_tables=False, case_sensitive_cols=False) %}
    {{ return(codegen.generate_source_starrocks(schema_name, database_name, catalog_name, generate_columns, include_descriptions, include_data_types, table_pattern, exclude, name, table_names, include_database, include_schema, case_sensitive_databases, case_sensitive_schemas, case_sensitive_tables, case_sensitive_cols)) }}
{% endmacro %}


{% macro generate_source_starrocks(schema_name, database_name, catalog_name, generate_columns, include_descriptions, include_data_types, table_pattern, exclude, name, table_names, include_database, include_schema, case_sensitive_databases, case_sensitive_schemas, case_sensitive_tables, case_sensitive_cols) %}

{% set sources_yaml=[] %}
{% do sources_yaml.append('version: 2') %}
{% do sources_yaml.append('') %}
{% do sources_yaml.append('sources:') %}
{% do sources_yaml.append('  - name: ' ~ name | lower) %}

{% if include_descriptions %}
    {% do sources_yaml.append('    description: ""' ) %}
{% endif %}

{# В StarRocks schema = catalog.database #}
{% set full_schema = catalog_name ~ '.' ~ schema_name %}
{% do sources_yaml.append('    schema: ' ~ full_schema) %}

{% do sources_yaml.append('    tables:') %}

{% if table_names is none %}
    {% set tables=codegen.get_tables_in_schema(schema_name, database_name) %}
{% else %}
    {% set tables = table_names %}
{% endif %}

{% for table in tables %}
    {% do sources_yaml.append('      - name: ' ~ (table if case_sensitive_tables else table | lower) ) %}
    {% if include_descriptions %}
        {% do sources_yaml.append('        description: ""' ) %}
    {% endif %}
    {% if generate_columns %}
    {% do sources_yaml.append('        columns:') %}

        {% set table_relation=api.Relation.create(
            database=database_name,
            schema=schema_name,
            identifier=table
        ) %}

        {% set columns=adapter.get_columns_in_relation(table_relation) %}

        {% for column in columns %}
            {% do sources_yaml.append('          - name: ' ~ (column.name if case_sensitive_cols else column.name | lower)) %}
            {% if include_data_types %}
                {% do sources_yaml.append('            data_type: ' ~ codegen.data_type_format_source(column)) %}
            {% endif %}
            {% if include_descriptions %}
                {% do sources_yaml.append('            description: ""' ) %}
            {% endif %}
        {% endfor %}
            {% do sources_yaml.append('') %}

    {% endif %}

{% endfor %}

{% if execute %}
    {% set joined = sources_yaml | join('\n') %}
    {{ print(joined) }}
    {% do return(joined) %}

{% endif %}

{% endmacro %}
