#/bin/bash

set -x
# This script bootstraps a local K8S based stack with SGA from the current code

eval $(minikube docker-env)
./docker/build.sh

# Start MinIO (S3 blobstorage)
kubectl apply -f helm/examples/minio-dev.yaml

# Start SGA
helm install sga helm/sga --values helm/examples/local.yaml
kubectl wait deployment/sga-control-plane --for condition=available --timeout=60s


# Start Kafka
kubectl create namespace kafka
kubectl create -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka
kubectl apply -f https://strimzi.io/examples/latest/kafka/kafka-persistent-single.yaml -n kafka
kubectl wait kafka/my-cluster --for=condition=Ready --timeout=300s -n kafka  

# Port forward SGA control plane
kubectl port-forward svc/sga-control-plane 8090:8090 &
