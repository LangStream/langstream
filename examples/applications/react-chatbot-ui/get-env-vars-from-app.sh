#!/bin/bash
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


# Receive YAML output from a pipe
YAML_OUTPUT=$(cat)

# Extract necessary values
APP=$(echo "$YAML_OUTPUT" | grep "application-id" | awk '{print $2}' | tr -d '"')

# Extract IDs for CONSUMER and PRODUCER based on the type
CONSUMER=$(echo "$YAML_OUTPUT" | awk '/- id: / {id=$3} /type: "consume"/ {print id}' | tr -d '"')
PRODUCER=$(echo "$YAML_OUTPUT" | awk '/- id: / {id=$3} /type: "produce"/ {print id}' | tr -d '"')

# Set environment variables
export REACT_APP_APP=$APP
export REACT_APP_CONSUMER=$CONSUMER
export REACT_APP_PRODUCER=$PRODUCER

# Print environment variables
echo "export REACT_APP_APP=$REACT_APP_APP"
echo "export REACT_APP_CONSUMER=$REACT_APP_CONSUMER"
echo "export REACT_APP_PRODUCER=$REACT_APP_PRODUCER"
