#
#
# Copyright DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import logging
import sys

import yaml

from . import runtime

if __name__ == "__main__":
    logging.addLevelName(logging.WARNING, "WARN")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d [%(threadName)s] %(levelname)-5s %(name).36s -- %(message)s",  # noqa: E501
        datefmt="%H:%M:%S",
    )

    print(sys.argv)
    if len(sys.argv) != 2:
        print("Missing pod configuration file argument")
        sys.exit(1)

    with open(sys.argv[1], "r") as file:
        config = yaml.safe_load(file)
        runtime.run_with_server(config)
