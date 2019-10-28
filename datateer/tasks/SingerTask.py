import platform
import subprocess

import prefect

class SingerTask(prefect.Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def run(self, tap, target, venv_root=None, tap_config_path=None, tap_catalog_path=None, tap_state_path=None, target_config_path=None, target_state_path=None) -> int:
        venv_root = venv_root or ''
        tap_command = [f'{venv_root}venv\\{tap}\\Scripts\\{tap}.exe' if platform.system() == 'Windows' else f'venv/{tap}/bin/{tap}']
        
        if tap_catalog_path is not None:
            tap_command.extend(['--catalog', tap_catalog_path])

        if tap_config_path is not None:
            tap_command.extend(['--config', tap_config_path])
        
        if tap_state_path is not None:
            tap_command.extend(['--state', tap_state_path])

        target_command = [f'{venv_root}venv\\{target}\\Scripts\\{target}.exe' if platform.system() == 'Windows' else f'venv/{target}/bin/{target}']
        if target_config_path is not None:
            target_command.extend(['--config', target_config_path])

        if target_state_path is not None:
            target_command.extend(['--state', target_state_path])

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
    
