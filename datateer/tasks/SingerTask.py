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
            tap_streams = subprocess.Popen(tap_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True) # https://stackoverflow.com/questions/2715847/read-streaming-input-from-subprocess-communicate/17698359#17698359
            output = subprocess.check_output(target_command, stdin=tap_streams.stdout) # pass the stdout stream from the tap to the target's stdin
            with tap_streams.stderr:
                for line in tap_streams.stderr:
                    self.logger.info(f'SINGER: {line}')

            # we use Popen to run the tap. If that encounters a non-zero return code, raise an exception so that we can consistently handle exceptions for the taps (started via Popen) and the targets (started via check_output)
            tap_streams.wait()
            if tap_streams.returncode != 0:
                raise subprocess.CalledProcessError(tap_streams.returncode, cmd=tap_command, output='The tap command encountered a problem. See the surrounding messages for details')

        except subprocess.CalledProcessError as exc:
            msg = f'Command failed with exit code {exc.returncode}{os.linesep}{exc.output}'
            self.logger.critical(f'Command failed with exit code {exc.returncode}')
            self.logger.critical(exc.output.decode('utf-8'))
            raise prefect.engine.signals.FAIL(msg) from None
        return output
    
