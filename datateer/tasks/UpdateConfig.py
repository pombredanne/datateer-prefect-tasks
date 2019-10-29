import json
import logging
import platform
import os
from shutil import copyfile
import subprocess

import boto3
import prefect

import datateer.tasks.util



class UpdateConfig(prefect.Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def run(self, flow_name, config_key, config_property, new_value) -> str:
        logger = logging.getLogger(__file__)
        env = os.getenv('DATATEER_ENV', 'local')
        if os.getenv('LOCAL_CONFIGS'):
            logger.warning(f'config {config_key} is in LOCAL_CONFIGS, so the property {config_property} will not be updated')
        else:
            ssm = boto3.client('ssm')
            path = f'/{flow_name}/{env}/{config_key}'
            param = ssm.get_parameter(Name=path, WithDecryption=True)
            config = json.loads(param['Parameter']['Value'])
            config['start_date'] = new_value
            params = ssm.put_parameter(Name=path, Value=json.dumps(config), Type='String', Overwrite=True)
            logger.info(f'update value in SSM for {path}')
           
