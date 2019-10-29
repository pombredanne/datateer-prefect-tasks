import platform
import os
import subprocess

import prefect

from datateer.tasks.util import get_root_dir
 
class SingerTask(prefect.Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run(self, tap=None, target=None, tap_config_path=None, tap_catalog_path=None, tap_state_path=None, target_config_path=None, target_state_path=None) -> int:
        root_dir = get_root_dir()

        self.logger.info(f'root_dir: {root_dir}')
        tap_command = [os.path.join(root_dir, f'venv\\{tap}\\Scripts\\{tap}.exe') if platform.system() == 'Windows' else os.path.join(root_dir, f'venv/{tap}/bin/{tap}')]
        
        if tap_catalog_path is not None:
            tap_command.extend(['--catalog', os.path.join(root_dir, tap_catalog_path)])

        if tap_config_path is not None:
            tap_command.extend(['--config', os.path.join(root_dir, tap_config_path)])
        
        if tap_state_path is not None:
            tap_command.extend(['--state', os.path.join(root_dir, tap_state_path)])

        target_command = [os.path.join(root_dir, f'venv\\{target}\\Scripts\\{target}.exe') if platform.system() == 'Windows' else os.path.join(root_dir, f'venv/{target}/bin/{target}')]
        if target_config_path is not None:
            target_command.extend(['--config', os.path.join(root_dir, target_config_path)])

        if target_state_path is not None:
            target_command.extend(['--state', os.path.join(root_dir, target_state_path)])

        self.logger.info('Running singer')
        self.logger.info(f'tap command: {tap_command}')
        self.logger.info(f'target command: {target_command}')

        try:
            input_stream = subprocess.Popen(tap_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, )
            output = subprocess.check_output(target_command, stdin=input_stream.stdout)
            out, err = input_stream.communicate() # connects to the stdout and stderr streams and receives their output. "out" will be empty because it was already streamed to the check_output command above. "err" contains informative logs from singer
            self.logger.info(err.decode('utf-8')) # this logs all the stderr info from singer. Singer uses stderr for informational messaging--more than just errors
        except subprocess.CalledProcessError as exc:
            msg = f'Command failed with exit code {exc.returncode}{os.linesep}{exc.output}'
            self.logger.critical(f'Command failed with exit code {exc.returncode}')
            self.logger.critical(exc.output.decode('utf-8'))
            raise prefect.engine.signals.FAIL(msg) from None
        return output
    
