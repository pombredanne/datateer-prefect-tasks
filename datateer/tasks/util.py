import os

import prefect

def get_root_dir():
    env = os.getenv('DATATEER_ENV')
    if env is None or env == '' or env != 'local':
        app_root = '/datateer/'
    else:
        app_root = os.getcwd()
    # logger = prefect.context.get('logger')
    # logger.info(f'DATATEER_ENV = {os.getenv("DATATEER_ENV")}')
    # logger.info(f'app_root = {app_root}')
    return app_root
    
