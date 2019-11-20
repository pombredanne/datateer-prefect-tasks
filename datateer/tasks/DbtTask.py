import platform
import os
import subprocess

import prefect

from datateer.tasks.util import get_root_dir


def generate_command_list(operations, models=[]):
    commands = []

    if 'debug' in operations:
        commands.append(['dbt', 'debug'])
    if 'deps' in operations:
        commands.append(['dbt', 'deps'])
    if 'run' in operations:
        c = ['dbt', 'run']
        if len(models):
            c.extend(['--models', *models])
        commands.append(c)
    if 'test' in operations:
        c = ['dbt', 'test']
        if len(models):
            c.extend(['--models', *models])
        commands.append(c)

    return commands

class DbtTask(prefect.Task):
    def __init__(self, operations, models=[], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.operations = operations
        self.models = models
    
    def run(self) -> str:
        
        if (os.getenv('DBT_PROFILES_DIR')):
            self.logger.info(f'dbt is using profiles directory {os.getenv("DBT_PROFILES_DIR")}')

        original_dir = os.getcwd()
        os.chdir(get_root_dir())
        self.logger.info(f'cwd is {os.getcwd()}')

        try:
            for command in generate_command_list(self.operations, self.models):
                self.logger.info(f'Running {command} from {os.getcwd()}')
                output = subprocess.check_output(command, stderr=subprocess.STDOUT)
                self.logger.info(output.decode('utf-8'))

            # input_stream.wait()
        except subprocess.CalledProcessError as exc:
            msg = f'Command failed with exit code {exc.returncode}{os.linesep}{exc.output}'
            self.logger.critical(f'Command failed with exit code {exc.returncode}')
            self.logger.critical(exc.output.decode('utf-8'))
            raise prefect.engine.signals.FAIL(msg) from None

        os.chdir(original_dir)

        return output
    
