import platform
import subprocess

import prefect

import datateer.tasks.util

class DbtTask(prefect.Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def run(self) -> str:
        dbt_command = [f'dbt', 'run']
        
        self.logger.debug(f'running dbt')

        try:
            output = subprocess.check_output(dbt_command, stderr=subprocess.STDOUT)
            # input_stream.wait()
        except subprocess.CalledProcessError as exc:
            msg = f'Command failed with exit code {exc.returncode}: {exc.output}'
            raise prefect.engine.signals.FAIL(msg) from None
        return output
    
