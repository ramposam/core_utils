import json
import os
from pathlib import Path

from ruamel.yaml import YAML


class DBTMirrorModel():
    def __init__(self, configs):
        self.configs = configs

    def generate_mirror_model(self, table_name, model_path, materialization, dataset_name, unique_key, schema,
                              database):
        """
        Generates a dbt model SQL file with the given configuration and source data.

        :param source_yaml_path: Path to the source YAML file
        :param model_path: Path to the output SQL file
        :param materialization: Materialization type (e.g., 'table')
        :param unique_key: Unique key for the dbt model
        :param schema: Schema name for the dbt model
        :param database: Database name for the dbt model
        """
        try:

            # Generate the dbt model SQL content
            sql_content = f"""
    {{{{ config(
        materialized="{materialization}",
        unique_key={unique_key},
        schema="{schema}",
        database="{database}"
    ) }}}}

{{%- set row_hash_excluded_columns = ['CREATED_BY', 'CREATED_DTS','FILE_DATE', 'FILENAME', 'FILE_ROW_NUMBER', 'FILE_LAST_MODIFIED','UPDATED_DTS','UPDATED_BY','UNIQUE_HASH_ID','ROW_HASH_ID'] -%}}
{{%- set excluded_columns = ['CREATED_BY', 'CREATED_DTS', 'UPDATED_DTS','UPDATED_BY','UNIQUE_HASH_ID','ROW_HASH_ID'] -%}}

WITH {dataset_name} AS (
    SELECT  
    {{{{  get_table_columns(this,excluded_columns)  }}}},
    md5({{{{  generate_unique_hash_id({unique_key})  }}}}) as unique_hash_id,
    md5({{{{  generate_row_hash_id(this,row_hash_excluded_columns)  }}}}) as row_hash_id,
    current_timestamp() as CREATED_DTS,
    current_user() as CREATED_BY,
    current_timestamp() as UPDATED_DTS,
    current_user() as UPDATED_BY

    FROM {{{{ source('mirror_{dataset_name}', '{table_name}_TR') }}}}
    where file_date = '{{{{ var("run_date")  }}}}'
)
SELECT *
FROM {dataset_name}
    """
            # Write the SQL content to the output file
            with open(model_path, 'w', encoding='utf-8') as sql_file:
                sql_file.write(sql_content.strip())

            print(f"Successfully generated dbt model SQL at {model_path}")
        except Exception as e:
            print(f"Error: {e}")

    def convert_json_to_yaml_preserve_order(self, json_data, yaml_file_path):
        """
        Converts a JSON file to a YAML file while preserving the order of elements.

        :param json_data: Path to the input JSON
        :param yaml_file_path: Path to the output YAML file
        """
        try:

            # Initialize ruamel.yaml for YAML handling
            yaml = YAML()
            yaml.default_flow_style = False

            # Write to the YAML file
            with open(yaml_file_path, 'w', encoding='utf-8') as yaml_file:
                yaml.dump(json_data, yaml_file)

            print(f"Successfully converted json data to {yaml_file_path} with preserved order.")
        except Exception as e:
            print(f"Error: {e}")

    def get_tests_yml(self, dataset_name, table_name, unique_keys, layer):

        mirror_template = {"name": dataset_name,
                           "version": 2,
                           "models": [{"name": table_name,
                                       "config": {"tags": [f"{dataset_name}-{layer}",
                                                           dataset_name]}}]}

        unique_table_level_column_tests = " || '-' || ".join(unique_keys)
        mirror_template["models"][0].update({"tests": [{"unique": {"column_name": unique_table_level_column_tests,
                                                                   "name": f"""{dataset_name}_{table_name}_unique""".upper(),
                                                                   "config": {"severity": "WARN",
                                                                              "where": """file_date = '{{ var("run_date") }}'"""}}}]})

        columns_test = []
        for column in unique_keys:
            columns_test.append({"name": column,
                                 "tests": [{"not_null": {"name": f"""{table_name}_{column}_not_null""".upper(),
                                                         "config": {"severity": "WARN",
                                                                    "where": """file_date = '{{ var("run_date") }}'"""}}}]})

        mirror_template["models"][0].update({"columns": columns_test})
        return mirror_template

    def get_sources_yml(self, dataset_name, table_name, database, schema, layer):
        source_template = {"name": dataset_name,
                           "version": 2,
                           "sources": [{"name": layer + "_" + dataset_name,
                                        "database": database,
                                        "schema": schema,
                                        "tables": [{"name": table_name,
                                                    "config": {"tags": [
                                                        f"{dataset_name}-src",
                                                        dataset_name]}
                                                    }]}]}

        return source_template

    def generate_mirror_source(self, mirror_table, mirror_dir, dataset_name, database, schema):

        mirror_sources_yml_path = os.path.join(mirror_dir, f"mirror_source_{dataset_name}.yml")
        mirror_source_data = self.get_sources_yml(dataset_name=dataset_name, table_name=f"{mirror_table}_TR",
                                                  database=database, schema=schema, layer="mirror")
        self.convert_json_to_yaml_preserve_order(mirror_source_data, mirror_sources_yml_path)

    def generate_mirror_tests(self, mirror_table, unique_keys, mirror_dir, dataset_name):

        mirror_table_tests_yml_path = os.path.join(mirror_dir, f"mirror_tests_{dataset_name}.yml")
        mirror_tests_data = self.get_tests_yml(dataset_name, f"{mirror_table}", unique_keys, "mirror")
        self.convert_json_to_yaml_preserve_order(mirror_tests_data, mirror_table_tests_yml_path)

    def generate_stage_source(self, mirror_table, stage_dir, dataset_name, database, schema):

        stage_sources_yml_path = os.path.join(stage_dir, f"stage_source_{dataset_name}.yml")
        stage_source_data = self.get_sources_yml(dataset_name=dataset_name, table_name=f"{mirror_table}",
                                                 database=database, schema=schema, layer="stage")
        self.convert_json_to_yaml_preserve_order(stage_source_data, stage_sources_yml_path)

    def generate_stage_tests(self, stage_table, unique_keys, stage_dir, dataset_name):

        stage_table_tests_yml_path = os.path.join(stage_dir, f"stage_tests_{dataset_name}.yml")
        stage_tests_data = self.get_tests_yml(dataset_name, f"{stage_table}", unique_keys, "stage")
        self.convert_json_to_yaml_preserve_order(stage_tests_data, stage_table_tests_yml_path)

    def getnerate_stage_model(self, table_name, mirror_table, model_path, materialization, dataset_name, unique_key,
                              schema,
                              database, transformations):

        # Generate the dbt model SQL content
        sql_content = f"""
            {{{{ config(
                materialized="{materialization}",
                unique_key={unique_key},
                schema="{schema}",
                database="{database}"
            ) }}}}

        {{%- set excluded_columns = ['CREATED_BY', 'CREATED_DTS','UPDATED_DTS', 'UPDATED_BY', 'UNIQUE_HASH_ID','ROW_HASH_ID'] -%}}
        """

        cte_queries = []
        cte_index = 0

        # Base CTE
        cte_queries.append(f""" cte_{cte_index} 
        AS ( 
        SELECT * exclude (CREATED_BY, CREATED_DTS,UPDATED_DTS, UPDATED_BY,FILENAME,FILE_DATE, FILE_ROW_NUMBER, FILE_LAST_MODIFIED, UNIQUE_HASH_ID, ROW_HASH_ID) 
        FROM  {{{{ source('stage_{dataset_name}', '{mirror_table}') }}}} 
        where file_date = '{{{{ var("run_date")  }}}}'
        )""")

        for transformation in transformations:
            cte_index += 1
            prev_cte = f"cte_{cte_index - 1}"

            if transformation['type'] == 'select':
                columns = ', '.join(transformation['columns'])
                query = f"SELECT {columns} FROM {prev_cte}"

            elif transformation['type'] == 'filter':
                condition = transformation['condition']
                query = f"SELECT * FROM {prev_cte} WHERE {condition}"

            elif transformation['type'] == 'join':
                join_table = transformation['table']
                join_condition = transformation['on']
                query = f"SELECT * FROM {prev_cte} JOIN {join_table} ON {join_condition}"

            elif transformation['type'] == 'pivot':
                column = transformation['column']
                values = ', '.join([f"'{v}'" for v in transformation['values']])
                query = (
                    f"SELECT * FROM crosstab(\n"
                    f"    'SELECT {column}, value FROM {prev_cte}',\n"
                    f"    ARRAY[{values}]\n"
                    f") AS ct({column} TEXT, value TEXT)"
                )

            elif transformation['type'] == 'unpivot':
                columns = ', '.join(transformation['columns'])
                alias = ', '.join(transformation['alias'])
                query = (
                    f"SELECT {alias} FROM {prev_cte} \n"
                    f"UNPIVOT ({alias} FOR column IN ({columns}))"
                )

            cte_queries.append(f"cte_{cte_index} AS (\n    {query}\n)")

        # Final query
        with_query = f"WITH\n" + ",\n".join(cte_queries) + f"""
        SELECT *, 
            'Y' as active_fl,
            '{{{{ var("run_date") }}}}' as effective_start_date ,
            '9999-12-31' as effective_end_date,
            md5({{{{  generate_unique_hash_id({unique_key})  }}}}) as unique_hash_id,
            md5({{{{  generate_row_hash_id(this,excluded_columns)  }}}}) as row_hash_id,
            current_timestamp() as CREATED_DTS,
            current_user() as CREATED_BY,
            current_timestamp() as UPDATED_DTS,
            current_user() as UPDATED_BY
       FROM cte_{cte_index}     

    """

        sql_content = sql_content + "\n" + with_query

        # Write the SQL content to the output file
        with open(model_path, 'w', encoding='utf-8') as sql_file:
            sql_file.write(sql_content.strip())

        print(f"Successfully generated dbt model SQL at {model_path}")

    def generate(self):

        current_dir = os.getcwd()
        models_path = os.path.join(current_dir, "dbt", "models")

        Path(models_path).mkdir(exist_ok=True, parents=True)

        dataset_name = list(self.configs.keys())[0]
        mirror_dir = os.path.join(models_path, "mirror", dataset_name)
        stage_dir = os.path.join(models_path, "stage", dataset_name)

        Path(mirror_dir).mkdir(exist_ok=True, parents=True)
        Path(stage_dir).mkdir(exist_ok=True, parents=True)

        mirror_configs = self.configs[dataset_name]["mirror"]
        stage_configs = self.configs[dataset_name]["stage"]

        mirror_table = mirror_configs["table_name"]
        stage_table = stage_configs["table_name"]

        unique_keys = mirror_configs["unique_keys"]

        self.generate_mirror_source(mirror_table, mirror_dir, dataset_name, mirror_configs["database"],
                                    mirror_configs["schema"])

        self.generate_mirror_tests(mirror_table, unique_keys, mirror_dir, dataset_name)

        mirror_model_table_path = os.path.join(mirror_dir, f"{mirror_table}.sql")

        self.generate_mirror_model(table_name=mirror_table,
                                   materialization="incremental",
                                   model_path=mirror_model_table_path,
                                   dataset_name=dataset_name,
                                   database=mirror_configs["database"],
                                   schema=mirror_configs["schema"],
                                   unique_key=unique_keys)

        self.generate_stage_source(mirror_table, stage_dir, dataset_name, mirror_configs["database"],
                                   mirror_configs["schema"])

        self.generate_stage_tests(stage_table, unique_keys, stage_dir, dataset_name)

        stage_model_table_path = os.path.join(stage_dir, f"{stage_table}.sql")

        self.getnerate_stage_model(table_name=stage_table,
                                   mirror_table=mirror_table,
                                   materialization="incremental",
                                   model_path=stage_model_table_path,
                                   dataset_name=dataset_name,
                                   database=stage_configs["database"],
                                   schema=stage_configs["schema"],
                                   unique_key=unique_keys,
                                   transformations=stage_configs["transformations"])


