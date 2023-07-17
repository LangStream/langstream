import yaml
import subprocess
import os

class PodPythonRuntime:
    def __init__(self, config_file):
        self.config_file = config_file
        self.config = None

    def load_config(self):
        if not os.path.exists(self.config_file):
            print("No configuration file provided. Using default `add.py`")
        else:
            with open(self.config_file, 'r') as file:
                self.config = yaml.safe_load(file)

    def execute(self, file_path):
        if self.config is None:
            pass
            # TODO: We probably want to disable execution if config isn't set. But for demo purposes, allowing.
        try:
            # Execute the Python file
            process = subprocess.Popen(['python', file_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            output, error = process.communicate()

            # Check if there was any error during execution
            if error:
                return error.decode('utf-8')
            
            # Return the output
            return output.decode('utf-8')
        
        except Exception as e:
            return str(e)


if __name__ == '__main__':
    # Example configuration loading
    runtime = PodPythonRuntime('config.yaml')
    runtime.load_config()

    # Example execution
    file_path = 'add.py'  # TODO: This comes from the configuration i assume
    result = runtime.execute(file_path)

    print(result)
