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

common_flags="-DskipTests -PskipPython -Dlicense.skip -Dspotless.skip -ntp"


build_docker_image() {
  module=$1
  clean="${2:-true}"
  clean_cmd=""
  if [ "$clean" == "true" ]; then
    clean_cmd="clean"
  fi
  ./mvnw install -am -pl $module -T 1C $common_flags
  ./mvnw $clean_cmd package -Pdocker -pl $module -Ddocker.platforms="$(docker_platforms)" $common_flags
  docker images | head -n 2
}

if [ "$only_image" == "control-plane" ]; then
  build_docker_image langstream-webservice
elif [ "$only_image" == "operator" ] || [ "$only_image" == "deployer" ]; then
  build_docker_image langstream-k8s-deployer/langstream-k8s-deployer-operator "false"
elif [ "$only_image" == "runtime-base-docker-image" ]; then
  build_docker_image langstream-runtime/langstream-runtime-base-docker-image
elif [ "$only_image" == "runtime" ]; then
  build_docker_image langstream-runtime/langstream-runtime-impl
elif [ "$only_image" == "runtime-tester" ]; then
  build_docker_image langstream-runtime/langstream-runtime-tester
elif [ "$only_image" == "cli" ]; then
  build_docker_image langstream-cli
elif [ "$only_image" == "api-gateway" ]; then
  build_docker_image langstream-api-gateway
else
  # Always clean to remove old NARs and cached docker images in the "target" directory
  ./mvnw clean install -Pdocker -Ddocker.platforms="$(docker_platforms)" $common_flags
  docker images | head -n 6
fi


