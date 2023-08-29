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
set -o errexit -o pipefail -o nounset
check_command() {
  if ! command -v $1 &> /dev/null
  then
    echo "Command $1 could not be found"
    exit
  fi
}

check_command docker
check_command minikube
check_command helm
check_command kubectl
check_command getopts


help() {
  echo "LangStream local cluster."
  echo
  echo "Syntax: $1 [-h]"
  echo "options:"
  echo "h     Print this Help."
  echo
}

minikube_profile=langstream-local-cluster
k8s_namespace=langstream
llr_dir=/tmp/llr
mkdir -p $llr_dir
kubeconfig_path=$llr_dir/kube-config
langstream_cli_config=$llr_dir/langstream-cli-config.yaml
touch $langstream_cli_config

if [ ! -f $llr_dir/dev-values.yaml ]; then
  cat > $llr_dir/dev-values.yaml <<EOF
controlPlane:
  image:
    pullPolicy: Never
    repository: langstream/langstream-control-plane
    tag: "latest-dev"
  app:
    config:
      logging.level.ai.langstream.webservice: debug
      application.storage.global.type: kubernetes

deployer:
  replicaCount: 1
  image:
    repository: langstream/langstream-deployer
    pullPolicy: Never
    tag: "latest-dev"
  app:
    config: {}

runtime:
  image: langstream/langstream-runtime:latest-dev
  imagePullPolicy: Never

client:
  replicaCount: 1
  image:
    repository: langstream/langstream-cli
    pullPolicy: Never
    tag: "latest-dev"

apiGateway:
  image:
    repository: langstream/langstream-api-gateway
    pullPolicy: Never
    tag: "latest-dev"
  app:
    config:
      logging.level.ai.langstream.apigateway: debug


codeStorage:
  type: s3
  configuration:
    endpoint: http://host.minikube.internal:9000
    access-key: minioadmin
    secret-key: minioadmin
EOF
fi

minikube_cmd() {
  KUBECONFIG=$llr_dir/kube-config minikube --profile $minikube_profile "$@"
}

start_minikube() {
  if minikube_cmd status | grep -q "host: Running"; then
    echo "Minikube: ✅"
    return
  fi
  minikube_cmd start --cpus 4
}

delete_minikube() {
  minikube_cmd delete
}

helm_cmd() {
  helm --kubeconfig $kubeconfig_path "$@"
}
kubectl_cmd() {
  KUBECONFIG=$kubeconfig_path kubectl "$@"
}

load_image_if_not_exists() {
  local image=$1
  echo -n "Image $image: "
  (minikube_cmd image list | grep -q $image) || load_image $image
  echo "✅"
}
load_image() {
  local image=$1
  minikube_cmd image load $image
}

install_langstream() {
  local dev="$1"
  local build="$2"
  helm_cmd repo add langstream https://datastax.github.io/langstream &> /dev/null || true
  helm_cmd repo update &> /dev/null
  if [ "$dev" == "true" ]; then
    if [ "$build" == "true" ]; then
      echo -n "Building images: "
      build_no_restart
      echo "✅"
    else
      load_image_if_not_exists langstream/langstream-control-plane:latest-dev
      load_image_if_not_exists langstream/langstream-cli:latest-dev
      load_image_if_not_exists langstream/langstream-deployer:latest-dev
      load_image_if_not_exists langstream/langstream-runtime:latest-dev
      load_image_if_not_exists langstream/langstream-api-gateway:latest-dev
    fi

    echo -n "LangStream: "
    helm_cmd upgrade --install langstream -n $k8s_namespace --create-namespace langstream/langstream --values $llr_dir/dev-values.yaml > /dev/null
  else
    echo -n "LangStream: "
    helm_cmd upgrade --install langstream langstream/langstream -n $k8s_namespace --create-namespace > /dev/null
  fi
  echo "✅"

}

start_port_forward() {
  local service=$1
  local file=$(mktemp)

  minikube_cmd service $1 --url -n $k8s_namespace &> $file &
  grep -q 'http' <(tail -f $file)
  url=$(grep -e http $file)
  echo "$url"
}

kafka_hostname=langstream-kafka

install_kafka() {
  if [ "$(docker ps -q -f name=$kafka_hostname)" ]; then
    echo "Kafka: ✅"
    return
  fi

  if [ "$(docker ps -a -q -f name=$kafka_hostname)" ]; then
    docker rm -f $kafka_hostname
  fi


  docker run \
    -d \
    --name $kafka_hostname \
    -p 39092:39092 \
    -e KAFKA_LISTENERS=BROKER://0.0.0.0:19092,EXTERNAL://0.0.0.0:39092,CONTROLLER://0.0.0.0:9093 \
    -e KAFKA_ADVERTISED_LISTENERS=BROKER://localhost:19092,EXTERNAL://host.minikube.internal:39092 \
    -e KAFKA_INTER_BROKER_LISTENER_NAME=BROKER \
    -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,BROKER:PLAINTEXT,EXTERNAL:PLAINTEXT \
    -e KAFKA_PROCESS_ROLES='controller,broker' \
    -e KAFKA_NODE_ID=1 \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS='1@0.0.0.0:9093' \
    -e KAFKA_LOG_DIRS='/tmp/kraft-combined-logs' \
    -e CLUSTER_ID=ciWo7IWazngRchmPES6q5A== \
    -v $llr_dir/kafka:/var/lib/kafka/data \
    confluentinc/cp-kafka:7.5.0
    echo "Kafka: ✅"
}

minio_hostname=langstream-minio

install_minio() {

  if [ "$(docker ps -q -f name=$minio_hostname)" ]; then
    echo "Minio: ✅"
    return
  fi

  if [ "$(docker ps -a -q -f name=$minio_hostname)" ]; then
    docker rm -f $minio_hostname
  fi
  docker run \
   -d \
   --name $minio_hostname \
   -p 9090:9090 \
   -v $llr_dir/minio:/data \
   quay.io/minio/minio:latest \
   server /data --console-address :9090
   echo "Minio: ✅"
}
configure_cli() {
  control_plane_url=$(start_port_forward langstream-control-plane)
  api_gateway_url=$(start_port_forward langstream-api-gateway)
  run_cli configure webServiceUrl $control_plane_url
  run_cli configure apiGatewayUrl $api_gateway_url
  run_cli configure tenant default
  echo "CLI: ✅"
}

run_cli() {
  LANGSTREAM_CLI_CONFIG=$langstream_cli_config langstream "$@"
}
start() {
  local dev="false"
  local build="false"
  while [[ "$#" -gt 0 ]]; do
      case $1 in
          --dev) dev="true"; shift ;;
          --build) build="true"; shift ;;
          *) echo "Unknown parameter passed: $1"; exit 1 ;;
      esac
  done
  start_minikube
  install_langstream "$dev" "$build"
  install_kafka
  install_minio
  configure_cli
}
delete() {
  docker rm -f $kafka_hostname
  docker rm -f $minio_hostname
  delete_minikube
  configure_cli
}

handle_load() {
  local image=$1
  if [[ $image == "control-plane" ]]; then
    image=langstream/langstream-control-plane:latest-dev
  elif [[ $image == "deployer" ]]; then
    image=langstream/langstream-deployer:latest-dev
  elif [[ $image == "api-gateway" ]]; then
    image=langstream/langstream-api-gateway:latest-dev
  elif [[ $image == "cli" ]]; then
    image=langstream/langstream-cli:latest-dev
  elif [[ $image == "runtime" ]]; then
    image=langstream/langstream-runtime:latest-dev
  fi
  load_image $image
  label=""
  if [[ $image == *"langstream-control-plane"* ]]; then
    label=langstream-control-plane
  elif [[ $image == *"langstream-deployer"* ]]; then
    label=langstream-deployer
  elif [[ $image == *"langstream-api-gateway"* ]]; then
    label=langstream-api-gateway
  elif [[ $image == *"langstream-cli"* ]]; then
    label=langstream-client
  fi
  if [ ! -z "$label" ]; then
    kubectl_cmd -n $k8s_namespace delete pod -l app.kubernetes.io/name=$label
    echo "Pod $label: ✅"
  fi
}
build_no_restart() {
  eval $(minikube_cmd docker-env)
  ./docker/build.sh "$@" &> /dev/null
  eval $(minikube_cmd docker-env -u)
}

handle_build() {
  eval $(minikube_cmd docker-env)
  ./docker/build.sh "$@" &> /dev/null
  eval $(minikube_cmd docker-env -u)

  labels=(langstream-control-plane langstream-deployer langstream-api-gateway langstream-client)
  for label in "${labels[@]}"; do
    kubectl_cmd -n $k8s_namespace delete pod -l app.kubernetes.io/name=$label &> /dev/null
    echo "Pod $label: ✅"
  done
}


arg1=$1
if [ "$arg1" == "start" ]; then
  shift
  start "$@"
elif [ "$arg1" == "delete" ]; then
  shift
  delete
elif [ "$arg1" == "langstream" ]; then
  shift
  run_cli "$@"
elif [ "$arg1" == "load" ]; then
  shift
  handle_load "$@"
elif [ "$arg1" == "build" ]; then
  shift
  handle_build "$@"
else
  echo "Unknown command $arg1"
  help
fi

