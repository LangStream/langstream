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
WEBSOCKET_URL=$(echo "$YAML_OUTPUT" | grep "apiGatewayUrl:" | awk '{print $2}' | tr -d '"')  
TENANT=$(echo "$YAML_OUTPUT" | grep "tenant:" | awk '{print $2}' | tr -d '"')
CREDENTIALS=$(echo "$YAML_OUTPUT" | grep "token:" | awk '{print $2}' | tr -d '"')  
  
# Set environment variables  
export REACT_APP_WEBSOCKET_URL=$WEBSOCKET_URL  
export REACT_APP_TENANT=$TENANT  
export REACT_APP_CREDENTIALS=$CREDENTIALS  
  
# Print environment variables  
echo "export REACT_APP_WEBSOCKET_URL=$REACT_APP_WEBSOCKET_URL"  
echo "export REACT_APP_TENANT=$REACT_APP_TENANT"  
