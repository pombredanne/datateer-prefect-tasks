import platform
import os
import subprocess

import prefect

import datateer.tasks.util

class DbtTask(prefect.Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def run(self) -> str:
        dbt_command = [f'dbt', 'run']
        
        if (os.getenv('DBT_PROFILES_DIR')):
            self.logger.info(f'dbt is using profiles directory {os.getenv("DBT_PROFILES_DIR")}')

        try:
            output = subprocess.check_output(dbt_command, stderr=subprocess.STDOUT)
            # input_stream.wait()
        except subprocess.CalledProcessError as exc:
            msg = f'Command failed with exit code {exc.returncode}{os.linesep}{exc.output}'
            self.logger.critical(f'Command failed with exit code {exc.returncode}')
            self.logger.critical(exc.output.decode('utf-8'))
            raise prefect.engine.signals.FAIL(msg) from None
        return output
    
