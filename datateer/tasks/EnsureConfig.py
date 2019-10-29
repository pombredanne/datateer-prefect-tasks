import json
import platform
import os
from shutil import copyfile
import subprocess

import boto3
import prefect

import datateer.tasks.util

# def sample(x, y):
#     return x + y

class EnsureConfig(prefect.Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def run(self, flow_name, config_key, config_directory=os.getcwd()) -> str:
        """Ensures a configuration file named <config_key> exists in <config_directory> by first checking the environment variable LOCAL_CONFIGS (a hash of key:local_dir) and copying the local file to the right config dir, and if not set or does not contain the config_key, looks to AWS SSM Parameter Store /app-name/env/config-key
        
        Arguments:
            config_key {string} -- The name of the configuration file
            config_directory {string} -- The destination directory where the configuration file should exist
        
        Returns:
            str -- [description]
        """

        env = os.getenv('DATATEER_ENV', 'local')
        if os.getenv('LOCAL_CONFIGS'):
            # configs = json.loads(os.getenv('LOCAL_CONFIGS'))
            configs = dict(item.split(':') for item in os.getenv('LOCAL_CONFIGS').split(','))
            return configs[config_key]
            # configs = dict((key.split(),val.split()) for key, val in (item.split(':') for item in os.getenv('LOCAL_CONFIGS').split(','))
            # if config_key in configs:
            #     src = os.path.join(config_directory, configs[config_key])
            #     dest = os.path.join(config_directory, config_key)
            #     copyfile(src, dest)
            #     return dest
        else:
            ssm = boto3.client('ssm')
            path = f'/{flow_name}/{env}/{config_key}'
            param = ssm.get_parameter(Name=path, WithDecryption=True)
            config_value = json.loads(param['Parameter']['Value'])
            dest = os.path.join(config_directory, config_key)
            with open(dest, 'w') as f:
                json.dump(config_value, f, indent=2)
            return dest

           
