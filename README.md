# LangStream

- [LangStream](#langstream)
  - [CLI](#cli)
    - [Installation](#installation)
    - [Enable auto-completion](#enable-auto-completion)
  - [Run locally](#run-in-local-kubernetes-minikube)

## CLI

### Installation
There are multiple ways to install the CLI.

- MacOS:
  - Homebrew
  ```
  brew install LangStream/langstream/langstream
  ```
  - Binary with curl
  ```
  curl -Ls "https://raw.githubusercontent.com/LangStream/langstream/main/bin/get-cli.sh" | bash
  ```  

- Unix:
  - Binary with curl
  ```
  curl -Ls "https://raw.githubusercontent.com/LangStream/langstream/main/bin/get-cli.sh" | bash
  ```  

### Enable auto-completion
Installing directly the binary will enable auto-completion for the CLI. If you installed the CLI with Homebrew, you can enable it with the following command:
```
[[ $(grep 'langstream generate-completion' "$HOME/.zshrc") ]] || echo -e "source <(langstream generate-completion)" >> "$HOME/.zshrc"
source $HOME/.zshrc # or open another terminal
```


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

NOTE: If the `kubectl apply` times out then you should take a look at your cluster with `k9s -A` and probably wait.

Deploy the LangStream Control Plane and the operator:

```
helm install langstream helm/langstream --values helm/examples/simple.yaml --wait --timeout 60s
```

NOTE: If the `helm install` times out then you should take a look at your cluster with `k9s -A` and possibly try with a larger timeout value.

Port forward control plane and the gateway to localhost:
```
kubectl port-forward svc/langstream-control-plane 8090:8090 &
kubectl port-forward svc/langstream-api-gateway 8091:8091 &
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

./bin/langstream apps deploy test -app examples/applications/compute-openai-embeddings -i examples/instances/kafka-kubernetes.yaml -s /tmp/secrets.yaml 
./bin/langstream apps get test
```

Check your k8s cluster with `k9s -A` or run `./bin/langstream apps get test` until the app is deployed.

Start producing messages:

```
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
minikube image load datastax/langstream-cli:latest-dev
minikube image load datastax/langstream-deployer:latest-dev
minikube image load datastax/langstream-control-plane:latest-dev
minikube image load datastax/langstream-runtime:latest-dev
minikube image load datastax/langstream-api-gateway:latest-dev
```

If you want to use the docker images you just built, use `helm/examples/local.yaml` values file:

```
helm install langstream helm/langstream --values helm/examples/local.yaml --wait --timeout 60s
```

## Deploying to GKE or similar K8s test cluster

Instead of `minio-dev.yaml` use the `helm/examples/minio-gke.yaml` file:

```
kubectl apply -f helm/examples/minio-gke.yaml
```

## Deploying to a Persistent Cluster

TODO: instructions on configuring with S3.
