# streaming-gen-ai
Streaming Gen AI Project

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

Build docker images in the minikube environment

```
eval $(minikube docker-env)
./docker/build.sh
```

Deploy MinIO (S3 BlobStorage implementation for local testing)

```
kubectl apply -f helm/examples/minio-dev.yaml
```

Deploy the control plane and the operator:

```
helm install sga helm/sga --values helm/examples/local.yaml
kubectl wait deployment/sga-control-plane --for condition=available --timeout=60s
```

Port forward control plane to localhost:
```
kubectl port-forward svc/sga-control-plane 8090:8090 &
```

Deploy Kafka:
```
kubectl create namespace kafka
kubectl create -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka
kubectl apply -f https://strimzi.io/examples/latest/kafka/kafka-persistent-single.yaml -n kafka
kubectl wait kafka/my-cluster --for=condition=Ready --timeout=300s -n kafka  
```

Create a sample app using the CLI:
```
./bin/sga-cli tenants put test
./bin/sga-cli configure test
./bin/sga-cli apps list


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

./bin/sga-cli apps deploy test -app examples/applications/app4 -i examples/instances/kafka-kubernetes.yaml -s /tmp/secrets.yaml 
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








