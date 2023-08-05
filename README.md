# Streaming Gen AI Project

# Quick Start

## Run in local Kubernetes (minikube)

### Requirements
- Minikube
- Helm
- Docker
- Java 17

```
brew install minikube
minikube start
```

Deploy MinIO (S3 BlobStorage implementation for local testing)

```
kubectl apply -f helm/examples/minio-dev.yaml
```

Deploy Kafka:
```
kubectl create namespace kafka
kubectl create -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka
kubectl apply -f https://strimzi.io/examples/latest/kafka/kafka-persistent-single.yaml -n kafka  
```

Deploy the SGAI Control Plane and the operator:

```
helm install sga helm/sga --values helm/examples/simple.yaml --wait --timeout 60s
```

Port forward control plane and the gateway to localhost:
```
kubectl port-forward svc/sga-control-plane 8090:8090 &
kubectl port-forward svc/sga-api-gateway 8091:8091 &
```

Wait for Kafka to be up and running:

```
kubectl wait kafka/my-cluster --for=condition=Ready --timeout=300s -n kafka
```

Create a sample app using the CLI:
```

OPEN_AI_URL=xx
OPEN_AI_ACCESS_KEY=xx

echo """
secrets:
  - name: open-ai
    id: open-ai
    data:
      url: $OPEN_AI_URL
      access-key: $OPEN_AI_ACCESS_KEY
""" > /tmp/secrets.yaml

./bin/sga-cli apps deploy test -app examples/applications/compute-openai-embeddings -i examples/instances/kafka-kubernetes.yaml -s /tmp/secrets.yaml 
./bin/sga-cli apps get test

kubectl -n kafka run kafka-producer -ti --image=quay.io/strimzi/kafka:0.35.1-kafka-3.4.0 --rm=true --restart=Never -- bin/kafka-console-producer.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 --topic input-topic
```

Insert a JSON with "name" and "description":

```
{"name": "test", "description": "test"}
```

In another terminal:

```
kubectl -n kafka run kafka-consumer -ti --image=quay.io/strimzi/kafka:0.35.1-kafka-3.4.0 --rm=true --restart=Never -- bin/kafka-console-consumer.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 --topic output-topic --from-beginning
```



## Building the docker images locally

Build docker images and push them into the minikube environment

```
./docker/build.sh
minikube image load datastax/sga-deployer:latest-dev
minikube image load datastax/sga-control-plane:latest-dev
minikube image load datastax/sga-runtime:latest-dev
minikube image load datastax/sga-api-gateway:latest-dev
```

If you want to use the docker images you just built, use `helm/examples/local.yaml` values file:

```
helm install sga helm/sga --values helm/examples/lcoal.yaml --wait --timeout 60s
```

## Deploying to GKE or similar K8s test cluster

Instead of `minio-dev.yaml` use the `helm/examples/minio-gke.yaml` file:

```
kubectl apply -f helm/examples/minio-gke.yaml
```

## Deploying to a Persistent Cluster

TODO: instructions on configuring with S3.
