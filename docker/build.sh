#!/bin/bash
set -e
only_image=$1

build_docker_image() {
  module=$1
  ./mvnw install -am -DskipTests -pl $module -T 1C
  ./mvnw package -DskipTests -Pdocker -pl $module
  docker images | head -n 2
}

if [ "$only_image" == "control-plane" ]; then
  build_docker_image webservice
elif [ "$only_image" == "operator" ]; then
  build_docker_image k8s-deployer/k8s-deployer-operator
elif [ "$only_image" == "runtime" ]; then
  build_docker_image runtime/runtime-impl
elif [ "$only_image" == "api-gateway" ]; then
  build_docker_image api-gateway
else
  ./mvnw package -am -DskipTests -Pdocker -T 1C
  docker images | head -n 4
fi


