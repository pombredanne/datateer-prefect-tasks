import platform
import os
import subprocess

import prefect

from datateer.tasks.util import get_root_dir
 
class SingerTask(prefect.Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.venv_dir = get_root_dir(os.path.join(os.getcwd(), 'venv'))

    def run(self, config_dir, tap, target, tap_config_path=None, tap_catalog_path=None, tap_state_path=None, target_config_path=None, target_state_path=None) -> int:
        self.logger.info(f'config_dir: {config_dir}')
        tap_command = [os.path.join(self.venv_dir, f'{tap}\\Scripts\\{tap}.exe') if platform.system() == 'Windows' else os.path.join(self.venv_dir, f'{tap}/bin/{tap}')]
        
        if tap_catalog_path is not None:
            tap_command.extend(['--catalog', os.path.join(config_dir, tap_catalog_path)])

        if tap_config_path is not None:
            tap_command.extend(['--config', os.path.join(config_dir, tap_config_path)])
        
        if tap_state_path is not None:
            tap_command.extend(['--state', os.path.join(config_dir, tap_state_path)])

        target_command = [os.path.join(self.venv_dir, f'{target}\\Scripts\\{target}.exe') if platform.system() == 'Windows' else os.path.join(self.venv_dir, f'{target}/bin/{target}')]
        if target_config_path is not None:
            target_command.extend(['--config', os.path.join(config_dir, target_config_path)])

        if target_state_path is not None:
            target_command.extend(['--state', os.path.join(config_dir, target_state_path)])

        self.logger.info(f'tap command: {tap_command}')
        self.logger.info(f'target command: {target_command}')

        try:
            input_stream = subprocess.Popen(tap_command, stdout=subprocess.PIPE)
            output = subprocess.check_output(target_command, stdin=input_stream.stdout)
            input_stream.wait()
        except subprocess.CalledProcessError as exc:
            msg = f'Command failed with exit code {exc.returncode}: {exc.output}'
            raise prefect.engine.signals.FAIL(msg) from None
        return output
    
