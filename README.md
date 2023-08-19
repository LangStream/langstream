# LangStream


* [LangStream](#langstream)
  * [Run LangStream server](#run-langstream-server)
    * [Quick start (All-in-one deployment)](#quick-start-all-in-one-deployment)
    * [LangStream deployment](#langstream-deployment)
  * [CLI](#cli)
    * [Installation](#installation)
    * [Enable auto-completion](#enable-auto-completion)
    * [Usage](#usage)
  * [Deploy your first application](#deploy-your-first-application)
  * [Development](#development)
    * [Start minikube](#start-minikube)
    * [Build docker images and deploy all the components](#build-docker-images-and-deploy-all-the-components)
    * [Deploying to GKE or similar K8s test cluster](#deploying-to-gke-or-similar-k8s-test-cluster)
    * [Deploying to a Persistent Cluster](#deploying-to-a-persistent-cluster)

## Run LangStream server
To run LangStream, you need to the following components:
- Apache Kafka or Apache Pulsar cluster
- S3 bucket or API-compatible storage
- Kubernetes Cluster (AWS EKS, Azure AKS, Google GKE, Minikube, etc.)

### Quick start (All-in-one deployment)

You can install all the required components in one-shot.
First, prepare the kubernetes local context:

```
kubectl config use-context minikube
```

Then, run the following command:

```
./dev/start-simple.sh
```

The above command will automatically start the port-forward for the LangStream control plane and the API Gateway.

### LangStream deployment
To install LangStream only, you can use the `langstream` Helm chart:

```
helm repo add langstream https://langstream.github.io/charts
helm repo update
helm install -n langstream --create-namespace langstream langstream/langstream --values helm/examples/simple.yaml
kubectl wait -n langstream deployment/langstream-control-plane --for condition=available --timeout=300s
```

You can then port-forward the control plane and the API Gateway:

```
kubectl port-forward svc/langstream-control-plane 8090:8090 &
kubectl port-forward svc/langstream-api-gateway 8091:8091 &
```



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

### Usage
To get started, run `langstream --help` to see the available commands.
By default, the CLI will connect to the control plane running on `localhost:8090`. You can change this by setting the `LANGSTREAM_webServiceUrl` environment variable.

To change permanently it, you can run:
```
langstream configure webServiceUrl <url>
```

The above applies for all the configuration options:

| Name          | Description                             | Default               |
|---------------|-----------------------------------------|-----------------------|
| webServiceUrl | The URL of the LangStream Control Plane | http://localhost:8090 |
| apiGatewayUrl | The URL of the LangStream API Gateway   | http://localhost:8091 |
| tenant        | The tenant to use                       | default               |
| token         | The token to use                        |                       |

To get your applications, run:
```
langstream apps list
```

## Deploy your first application

Inside the [examples](./examples) folder, you can find some examples of applications.

In this example, you will deploy an application that performs AI completion and return information about a known person.

1. Create the secrets file containing the OpenAI URL and access key:
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
```

2. Deploy the `openai-completions` application

```
./bin/langstream apps deploy openai-completions -app examples/applications/openai-completions -i examples/instances/kafka-kubernetes.yaml -s /tmp/secrets.yaml
./bin/langstream apps get openai-completions
```

Check your k8s cluster with `k9s -A` or run `./bin/langstream apps get openai-completions` until the app is deployed.

Test the AI completion using the API gateway. Pass a person name as input to get information about them:
```
session="$(uuidgen)"
./bin/langstream gateway produce openai-completions produce-input -p sessionId="$session" -v "Barack Obama"
./bin/langstream gateway consume openai-completions consume-output -p sessionId="$session"
```

Another approach to test values is to use gateway `chat` CLI feature:
```
./bin/langstream gateway chat openai-completions -cg consume-output -pg produce-input -p sessionId=$(uuidgen)
```

## Development

Requirements:
- Minikube
- Helm
- Docker
- Java 17

### Start minikube
  
```
brew install minikube
minikube start
```

### Build docker images and deploy all the components
```
./dev/start-local.sh
```

The above command will automatically start the port-forward for the LangStream control plane and the API Gateway.


### Deploying to GKE or similar K8s test cluster

Instead of `minio-dev.yaml` use the `helm/examples/minio-gke.yaml` file:

```
kubectl apply -f helm/examples/minio-gke.yaml
```

### Deploying to a Persistent Cluster

TODO: instructions on configuring with S3.
