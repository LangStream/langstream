#/bin/bash

cd $(dirname $0)/..

set -ex
# This script bootstraps a local K8S based stack with SGA from the current code


SKIP_BUILD=${SKIP_BUILD:-"false"}

use_minikube_load() {
  if [ "$(uname -m)" == "arm64" ]; then
    echo "true"
  else
    echo "false"
  fi
}

if [ "true" == "$(use_minikube_load)" ]; then
  if [ "$SKIP_BUILD" == "false" ]; then
    . ./docker/build.sh
  fi
  minikube image load --overwrite datastax/sga-deployer:latest-dev
  minikube image load --overwrite datastax/sga-control-plane:latest-dev
  minikube image load --overwrite datastax/sga-runtime:latest-dev
  minikube image load --overwrite datastax/sga-api-gateway:latest-dev
else
  eval $(minikube docker-env)
  if [ "$SKIP_BUILD" == "false" ]; then
    . ./docker/build.sh
  fi
fi

# Start MinIO (S3 blobstorage)
kubectl apply -f helm/examples/minio-dev.yaml

# Start SGA
kubectl apply -f helm/sga/crds
helm install sga helm/sga --values helm/examples/local.yaml || helm upgrade sga helm/sga --values helm/examples/local.yaml
kubectl wait deployment/sga-control-plane --for condition=available --timeout=300s


# Start Kafka
kubectl create namespace kafka || true
kubectl apply -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka
kubectl apply -f https://strimzi.io/examples/latest/kafka/kafka-persistent-single.yaml -n kafka
kubectl wait kafka/my-cluster --for=condition=Ready --timeout=300s -n kafka  

# Port forward SGA control plane
pkill -f "kubectl port-forward svc/sga-control-plane 8090:8090" || true
kubectl port-forward svc/sga-control-plane 8090:8090 &
