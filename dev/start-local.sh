#/bin/bash
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

cd $(dirname $0)/..

set -ex
# This script bootstraps a local K8S based stack with LangStream from the current code


SKIP_BUILD=${SKIP_BUILD:-"false"}

use_minikube_load() {
  if [ "$(uname -m)" == "arm64" ]; then
    echo "true"
  else
    echo "false"
  fi
}

# Start Kafka, it takes some time
kubectl create namespace kafka || true
kubectl apply -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka
kubectl apply -f https://strimzi.io/examples/latest/kafka/kafka-persistent-single.yaml -n kafka

# Load the local CRDs before loading them with the Helm Chart (they will take precedence)
for i in helm/crds/*yml ; do
   echo "loading CRD $i"
   kubectl apply -f $i
done


if [ "true" == "$(use_minikube_load)" ]; then
  if [ "$SKIP_BUILD" == "false" ]; then
    . ./docker/build.sh
  fi
  minikube image load --overwrite langstream/langstream-deployer:latest-dev
  minikube image load --overwrite langstream/langstream-control-plane:latest-dev
  minikube image load --overwrite langstream/langstream-runtime:latest-dev
  minikube image load --overwrite langstream/langstream-api-gateway:latest-dev
  minikube image load --overwrite langstream/langstream-cli:latest-dev
else
  eval $(minikube docker-env)
  if [ "$SKIP_BUILD" == "false" ]; then
    . ./docker/build.sh
  fi
fi

# Start MinIO (S3 blobstorage)
kubectl apply -f helm/examples/minio-dev.yaml

# Start HerdDB (Simple Vector Database)
kubectl apply -f helm/examples/herddb-dev.yaml

# Start LangStream
helm repo add langstream https://langstream.github.io/charts
helm repo update
helm install langstream langstream/langstream --values helm/examples/local.yaml || helm upgrade langstream langstream/langstream --values helm/examples/local.yaml
kubectl wait deployment/langstream-control-plane --for condition=available --timeout=300s


# Wait for Kafka to be up and running
kubectl wait kafka/my-cluster --for=condition=Ready --timeout=300s -n kafka  

# Port forward LangStream control plane and Gateway
pkill -f "kubectl port-forward svc/langstream-control-plane 8090:8090" || true
kubectl port-forward svc/langstream-control-plane 8090:8090 &

pkill -f "kubectl port-forward svc/langstream-api-gateway 8091:8091" || true
kubectl port-forward svc/langstream-api-gateway 8091:8091 &

