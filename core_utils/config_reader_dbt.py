import json
import os
from typing import List, Dict, Any
from datetime import datetime


class ConfigReaderDBT():

    def __init__(self,dataset_configs_path,dataset_name,run_date):
        self.dataset_configs_path = dataset_configs_path
        self.dataset_name = dataset_name
        self.run_date = run_date

    def read_json_file(self,file_path: str) -> Dict[str, Any]:
        """
        Reads a JSON file and returns its content as a dictionary.

        :param file_path: Path to the JSON file
        :return: Dictionary with JSON data
        """
        with open(file_path, 'r', encoding='utf-8') as file:
            return json.load(file)


    def get_current_version(self,dataset):
        """
        Filters the current date between start_date and end_date and returns the version.

        :param dataset: Dictionary containing dataset details with versions
        :return: The version string if current date is within range, otherwise None
        """
        current_date = datetime.strptime(self.run_date, '%Y-%m-%d').date()
        versions = dataset.get('versions', [])

        for version_info in versions:
            start_date = datetime.strptime(version_info['start_date'], '%Y-%m-%d').date()
            end_date = datetime.strptime(version_info['end_date'], '%Y-%m-%d').date()

            if start_date <= current_date <= end_date:
                return version_info['version']

        return None


    def get_mirror_configs(self):
        mirror_configs_dir = os.path.join(self.dataset_configs_path,"mirror")
        mirror_ver_file_path = os.path.join(mirror_configs_dir,f"{self.dataset_name}_mirror_ver.json")
        mirror_versions = self.read_json_file(mirror_ver_file_path)

        mirror_configs = {}

        for index,details in enumerate(mirror_versions["versions"]):
            mirror_configs_file_path = os.path.join(mirror_configs_dir, f"""{self.dataset_name}_mirror_{details["version"]}.json""")
            mirror_ver_configs = self.read_json_file(mirror_configs_file_path)
            mirror_configs[details["version"]] = mirror_ver_configs

        return mirror_versions,mirror_configs


    def get_stage_configs(self):
        stage_configs_dir = os.path.join(self.dataset_configs_path, "stage")
        stage_ver_file_path = os.path.join(stage_configs_dir, f"{self.dataset_name}_stage_ver.json")
        stage_versions = self.read_json_file(stage_ver_file_path)

        stage_configs = {}

        for index, details in enumerate(stage_versions["versions"]):
            stage_configs_file_path = os.path.join(stage_configs_dir, f"""{self.dataset_name}_stage_{details["version"]}.json""")
            stage_ver_configs = self.read_json_file(stage_configs_file_path)
            stage_configs[details["version"]] = stage_ver_configs

        return stage_versions, stage_configs

    def get_configs(self):
        source_json_file_path = os.path.join(self.dataset_configs_path,f"{self.dataset_name}.json")
        source_configs = self.read_json_file(source_json_file_path)

        mirror_versions,mirror_configs = self.get_mirror_configs()

        stage_versions, stage_configs = self.get_stage_configs()

        mirror_version = self.get_current_version(mirror_versions)
        stage_version = self.get_current_version(stage_versions)

        dataset_configs  = {self.dataset_name:{"mirror":{"database":source_configs["mirror_layer"]["database"],
                                            "schema":source_configs["mirror_layer"]["schema"],
                                            "table_name":mirror_configs[mirror_version]["table_name"],
                                            "table_schema":mirror_configs[mirror_version]["table_schema"],
                                            "file_schema": mirror_configs[mirror_version]["file_schema"],
                                            "unique_keys":mirror_configs[mirror_version]["unique_keys"],
                                            "file_format_params":mirror_configs[mirror_version]["file_format_params"]},
                                  "stage":{"database":source_configs["stage_layer"]["database"],
                                            "schema":source_configs["stage_layer"]["schema"],
                                            "table_name":stage_configs[stage_version]["table_name"],
                                            "table_schema":stage_configs[stage_version]["table_schema"],
                                           "unique_keys":stage_configs[stage_version]["unique_keys"]}}}
        return dataset_configs
