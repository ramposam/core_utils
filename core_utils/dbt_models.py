import os
from pathlib import Path

from ruamel.yaml import YAML

class DBTMirrorModel():
    def __init__(self,configs):
        self.configs = configs

    def generate_dbt_model_sql(self,table_name, model_path, materialization, dataset_name, unique_key, schema,
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

    WITH {dataset_name} AS (
        SELECT *
        FROM {{{{ source('{dataset_name}', '{table_name}_TR') }}}}
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

    def convert_json_to_yaml_preserve_order(self,json_data, yaml_file_path):
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

    def get_tests_yml(self,dataset_name,mirror_table,unique_keys):

        mirror_template = {"name": dataset_name,
                           "version": 2,
                           "models": [{"name": mirror_table,
                                      "config": {"tags": [f"{dataset_name}-mirror",
                                                          dataset_name]}}]}

        unique_table_level_column_tests = " || '-' || ".join(unique_keys)
        mirror_template["models"][0].update({"tests": [{"unique": {"column_name": unique_table_level_column_tests,
                                                                "name": f"""{dataset_name}_{mirror_table}_unique""".upper(),
                                                                "config": {"severity": "WARN",
                                                                           "where": """file_date = '{{ var("run_date") }}'"""}}}]})

        columns_test = []
        for column in unique_keys:
            columns_test.append({"name": column,
                                 "tests": [{"not_null": {"name": f"""{mirror_table}_{column}_not_null""".upper(),
                                                         "config": {"severity": "WARN",
                                                                    "where": """file_date = '{{ var("run_date") }}'"""}}}]})

        mirror_template["models"][0].update({"columns": columns_test})
        return mirror_template

    def get_sources_yml(self,dataset_name,table_name):
        source_template = {"name": dataset_name,
                           "version": 2,
                           "sources": [{"name": dataset_name,
                                       "database": "MIRROR_DB",
                                      "schema": "MIRROR",
                                       "tables":[{"name": table_name,
                                                  "config": {"tags": [
                                                      f"{dataset_name}-src",
                                                      dataset_name]}
                                                  }]}]}

        return source_template

    def generate(self):

        models_path = "models"
        dataset_name = list(self.configs.keys())[0]
        mirror_dir = os.path.join(os.getcwd(), models_path, "mirror", dataset_name)

        Path(mirror_dir).mkdir(exist_ok=True, parents=True)

        mirror_configs = self.configs[dataset_name]["mirror"]

        mirror_table = mirror_configs["table_name"]
        unique_keys = mirror_configs["unique_keys"]

        mirror_sources_yml_path = os.path.join(mirror_dir, f"mirror_source_{dataset_name}.yml")
        mirror_source_data = self.get_sources_yml(dataset_name=dataset_name,table_name=f"{mirror_table}_TR")
        self.convert_json_to_yaml_preserve_order(mirror_source_data, mirror_sources_yml_path)

        mirror_table_tests_yml_path = os.path.join(mirror_dir, f"mirror_tests_{dataset_name}.yml")
        mirror_tests_data = self.get_tests_yml(dataset_name,f"{mirror_table}",unique_keys)
        self.convert_json_to_yaml_preserve_order(mirror_tests_data, mirror_table_tests_yml_path)

        mirror_model_table_path = os.path.join(mirror_dir, f"{mirror_table}.sql")
        self.generate_dbt_model_sql(table_name=mirror_table,
                               materialization="incremental",
                               model_path=mirror_model_table_path,
                               dataset_name=dataset_name,
                               database=mirror_configs["database"],
                               schema=mirror_configs["schema"],
                               unique_key=unique_keys)


