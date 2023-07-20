
import sys

import yaml

import sga_runtime

if __name__ == '__main__':
    print(sys.argv)
    if len(sys.argv) != 2:
        print("Missing pod configuration file argument")
        sys.exit(1)

    with open(sys.argv[1], 'r') as file:
        config = yaml.safe_load(file)
        sga_runtime.run(config)
