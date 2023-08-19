# LangStream

# Quick Start for local application development

1. Make sure minikube is installed and started
2. Make sure kubectl, Helm are installed
3. Install Mino, Kafka in minikube
4. Install the LangStream Helm chart in minikube. 
5. Forward the control plane and API ports from minikube
6. Install the langstream CLI
7. Deploy sample app
8. Use sample app


# Set up for local LangStream project development

## Development Requirements
For project development, you need the following:

- Java 17
- Python 3.8

## Run in local Kubernetes (minikube)

### Requirements

LangStream runs in Kubernetes. To use it locally, you need a local Kubernetes environment. Minikube is a good
choice for that.


### Setting up a local Kubernetes environment

For your local Kubernetes environment, you need the following:
* Minikube
* Kubectl
* Helm

#### Minikube on MacOS

To get started with Minikube on MacOS:

1. Install Homebrew.

   If you haven't installed Homebrew yet, use the following command:

```
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

2. Update Homebrew.

   It's a good practice to ensure you get the latest packages.

```
brew update
```

3. Install Minikube.

   Use the following command to install MiniKube.

```
brew install minikube
```

4. Install a Hypervisor

Minikube requires a hypervisor to create a virtual machine where the Kubernetes cluster will run. 
Here's how to install HyperKit, which is recommended for macOS:

```
brew install hyperkit
```

After installation, set Minikube to use HyperKit and 4 CPUs:

```
minikube config set driver hyperkit
minikube config set cpus 4
```

5. Start Minikube:

To start your local Kubernetes cluster:

```
minikube start
```

For additional information on installing minikube or installing in other environments, 
see this [page](https://minikube.sigs.k8s.io/docs/start/).

If you no longer need Minikube, you
can stop (`minikube stop`) and delete the cluster (`minikube delete`).

#### Kubectl

To install kubectl, use the following command:
```
brew install kubectl
```

For additional information on installing kubectl or installing in other environments, 
see this [page](https://kubernetes.io/docs/tasks/tools/#kubectl).

#### Helm

To install Helm, use the following command:

```
brew install helm
```

For additional information on installing Helm or installing in other environments, 
see this [page](https://helm.sh/docs/intro/install/).

### Deploying local MinIO and Kafka

Deploy MinIO (S3 BlobStorage implementation for local testing). From the root directory of the repository:

```
kubectl apply -f helm/examples/minio-dev.yaml
```

Make sure the minio pod is running using the command `kubectl get pods -n minio-dev`.

Deploy Kafka:
```
kubectl create namespace kafka
kubectl create -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka
kubectl apply -f https://strimzi.io/examples/latest/kafka/kafka-persistent-single.yaml -n kafka 
```
Wait for Kafka to be up and running:

```
kubectl wait kafka/my-cluster --for=condition=Ready --timeout=300s -n kafka
```

NOTE: If the `kubectl wait` times out then you should take a look at pods in the kafka namespace using `kubectl get pods -n kafka` and 
check if they are just slow to go into the Running state. For more information on getting started with the Strimzi Kafka operator, 
see this [page](https://strimzi.io/quickstarts/).

### Build the project and containers locally

Build docker images and push them into the minikube environment:

```
./docker/build.sh
minikube image load datastax/langstream-cli:latest-dev
minikube image load datastax/langstream-deployer:latest-dev
minikube image load datastax/langstream-control-plane:latest-dev
minikube image load datastax/langstream-runtime:latest-dev
minikube image load datastax/langstream-api-gateway:latest-dev
```

Note that loading images into minikube can take several minutes.

### Deploy the LangStream control plane and operator

The following command will install LangStream from the local repository. You must be in the root of the repository 
when running the command.

```
helm install langstream helm/langstream --values helm/examples/local.yaml --wait --timeout 120s
```

NOTE: If the `helm install` times out then you should take a look at the pods in the langstream namespace using `kubectl get pods -n langstream` 
and check if they are just slow to go into the running state.

### Port forward the control plane

To port-forward control plane and the gateway to localhost, use the following commands:
```
kubectl port-forward svc/langstream-control-plane 8090:8090 &
kubectl port-forward svc/langstream-api-gateway 8091:8091 &
```

### Build and test the CLI

To use the langstream CLI, you need to build it. From the root of the repository:

```
mvn build -DskipTests
```

Test the CLI:

```
./bin/langstream tenants list
```

This should return a tenant named `default`.

### Deploy a sample LangStream application

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

Run `./bin/langstream apps get test` until the app is deployed or check your k8s cluster with `kubectl get pods -A` to see if 
all pods are in the running state. Note this can take several minutes to download the container images.

### Use the LangStream application

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

## Deploying to GKE or similar K8s test cluster

Instead of `minio-dev.yaml` use the `helm/examples/minio-gke.yaml` file:

```
kubectl apply -f helm/examples/minio-gke.yaml
```

## Deploying to a Persistent Cluster

TODO: instructions on configuring with S3.