import platform
import os
import subprocess

import prefect

from datateer.tasks.util import get_root_dir

class DbtTask(prefect.Task):
    def __init__(self, run_data=True, run_tests=True, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.run_data = run_data
        self.run_tests = run_tests
    
    def run(self) -> str:
        dbt_deps = ['dbt', 'deps']
        dbt_debug = ['dbt', 'debug']
        dbt_run = ['dbt', 'run']
        dbt_test = ['dbt', 'test']
        
        if (os.getenv('DBT_PROFILES_DIR')):
            self.logger.info(f'dbt is using profiles directory {os.getenv("DBT_PROFILES_DIR")}')

        original_dir = os.getcwd()
        os.chdir(get_root_dir())
        self.logger.info(f'cwd is {os.getcwd()}')

        try:
            if self.run_data:
                self.logger.info('Running dbt deps')
                output = subprocess.check_output(dbt_deps, stderr=subprocess.STDOUT)
                self.logger.info(output.decode('utf-8'))
                self.logger.info('Running dbt debug')
                output = subprocess.check_output(dbt_debug, stderr=subprocess.STDOUT)
                self.logger.info(output.decode('utf-8'))
                self.logger.info('Running dbt run')
                output = subprocess.check_output(dbt_run, stderr=subprocess.STDOUT)
                self.logger.info(output.decode('utf-8'))
            if self.run_tests:
                self.logger.info('Running dbt test')
                output = subprocess.check_output(dbt_test, stderr=subprocess.STDOUT)
                self.logger.info(output.decode('utf-8'))

            # input_stream.wait()
        except subprocess.CalledProcessError as exc:
            msg = f'Command failed with exit code {exc.returncode}{os.linesep}{exc.output}'
            self.logger.critical(f'Command failed with exit code {exc.returncode}')
            self.logger.critical(exc.output.decode('utf-8'))
            raise prefect.engine.signals.FAIL(msg) from None

        os.chdir(original_dir)

        return output
    
