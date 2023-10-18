#!/bin/bash
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

set -e
agent_module=$1
if [ -z "$agent_module" ]; then
  echo "Specify an agent module"
  exit 1
fi

mvn clean install -DskipTests -PskipPython -Dlicense.skip -Dspotless.skip -ntp -pl langstream-agents/$agent_module
cd langstream-agents/$agent_module/target
cd ..
docker build -t langstream/langstream-runtime-tester:latest-dev -f- . <<EOF
FROM langstream/langstream-runtime-tester:latest-dev
USER 0
RUN rm -rf /app/agents/${agent_module}-*
COPY target/*.nar /app/agents/
USER 10000
EOF