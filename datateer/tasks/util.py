import os

import prefect

def get_root_dir():
    if os.getenv('DATATEER_ENV').startswith('local'):
        app_root = os.getcwd()
    else:
        app_root = '/datateer/'
    return app_root
    
