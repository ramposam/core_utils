import json
import os


class ConfigReader():
    def __init__(self, config_path, dataset_name):
        self.config_path = config_path
        self.dataset_name = dataset_name

    def read_configs(self):
        # Load configuration template
        dataset_config_json_path = os.path.join(self.config_path, f"{self.dataset_name}.json")
        with open(dataset_config_json_path, 'r') as file:
            dataset_configs = json.load(file)

        dataset_configs_mirror_ver_path = os.path.join(self.config_path, "mirror",
                                                       f"{self.dataset_name}_mirror_ver.json")
        with open(dataset_configs_mirror_ver_path, 'r') as file:
            mirror_ver_configs = json.load(file)
        versions = mirror_ver_configs["versions"][0]

        dataset_configs_mirror_v1_path = os.path.join(self.config_path, "mirror",
                                                      f"{self.dataset_name}_mirror_v1.json")
        with open(dataset_configs_mirror_v1_path, 'r') as file:
            mirror_v1_configs = json.load(file)
        v1_configs = mirror_v1_configs

        mirror_configs = {**versions, **v1_configs}

        dataset_configs["mirror"] = {"v1": mirror_configs}

        dataset_configs_stage_ver_path = os.path.join(self.config_path, "stage",
                                                      f"{self.dataset_name}_stage_ver.json")
        with open(dataset_configs_stage_ver_path, 'r') as file:
            stage_ver_configs = json.load(file)
        versions = stage_ver_configs["versions"][0]

        dataset_configs_stage_v1_path = os.path.join(self.config_path, "stage",
                                                     f"{self.dataset_name}_stage_v1.json")
        with open(dataset_configs_stage_v1_path, 'r') as file:
            stage_v1_configs = json.load(file)
        v1_configs = stage_v1_configs

        stage_configs = {**versions, **v1_configs}

        dataset_configs["stage"] = {"v1": stage_configs}

        return dataset_configs