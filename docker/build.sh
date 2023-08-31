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
only_image=$1

docker_platforms() {
  if [ "$(uname -m)" == "arm64" ]; then
    echo "linux/amd64"
  else
    echo ""
  fi
}


build_docker_image() {
  module=$1
  ./mvnw clean install -am -DskipTests -pl $module -T 1C -PskipPython
  ./mvnw package -DskipTests -Pdocker -pl $module -Ddocker.platforms="$(docker_platforms)" -PskipPython
  docker images | head -n 2
}

if [ "$only_image" == "control-plane" ]; then
  build_docker_image langstream-webservice
elif [ "$only_image" == "operator" ] || [ "$only_image" == "deployer" ]; then
  build_docker_image langstream-k8s-deployer/langstream-k8s-deployer-operator
elif [ "$only_image" == "runtime" ]; then
  build_docker_image langstream-runtime/langstream-runtime-impl
elif [ "$only_image" == "cli" ]; then
  build_docker_image langstream-cli
elif [ "$only_image" == "api-gateway" ]; then
  build_docker_image langstream-api-gateway
else
  # Build all artifacts
  ./mvnw install -DskipTests -T 1C -Ddocker.platforms="$(docker_platforms)" -PskipPython
  # Build docker images
  ./mvnw package -DskipTests -Pdocker -T 1C -Ddocker.platforms="$(docker_platforms)" -PskipPython
  docker images | head -n 6
fi


