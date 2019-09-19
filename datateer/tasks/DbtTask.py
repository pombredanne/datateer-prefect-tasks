import platform
import subprocess

import prefect

class DbtTask(prefect.Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def run(self, profile_path=None) -> str:
        dbt_command = [f'dbt', 'run']
        if profile_path is not None:
            raise 'profile_path is not yet implemented'
        
        self.logger.debug(f'running dbt')

        try:
            output = subprocess.check_output(dbt_command, stderr=subprocess.STDOUT)
            # input_stream.wait()
        except subprocess.CalledProcessError as exc:
            msg = f'Command failed with exit code {exc.returncode}: {exc.output}'
            raise prefect.engine.signals.FAIL(msg) from None
        return output
    
