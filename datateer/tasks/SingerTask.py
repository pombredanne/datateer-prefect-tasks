import platform
import subprocess

import prefect

class SingerTask(prefect.Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def run(self, tap, target, tap_config_path=None, tap_state_path=None, target_config_path=None, target_state_path=None) -> int:
        tap_command = [f'venv\\{tap}\\Scripts\\{tap}.exe' if platform.system() == 'Windows' else f'venv/{tap}/bin/{tap}']
        if tap_config_path is not None:
            tap_command.extend(['--config', tap_config_path])
        
        target_command = [f'venv\\{target}\\Scripts\\{target}.exe' if platform.system() == 'Windows' else f'venv/{target}/bin/{target}']
        if target_config_path is not None:
            target_command.extend(['--config', target_config_path])

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
    
