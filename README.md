# LangStream
[Have a question? Join our Slack channel!](https://join.slack.com/t/langstream/shared_invite/zt-21leloc9c-lNaGLdiecHuWU5N31L2AeQ)

For the complete documentation, go [here](https://docs.langstream.ai)

Get the LangStream VS Code extension [here](https://marketplace.visualstudio.com/items?itemName=DataStax.langstream).


## Contents

* [LangStream](#langstream)
  * [Run LangStream server](#run-langstream-server)
    * [Quick start (All-in-one deployment)](#quick-start-all-in-one-deployment)
    * [LangStream deployment](#langstream-deployment)
  * [CLI](#cli)
    * [Installation](#installation)
    * [Enable auto-completion](#enable-auto-completion)
    * [Usage](#usage)
  * [Deploy your first application](#deploy-your-first-application)
  * [Install Kubernetes Environment on MacOS](#install-kubernetes-environment-on-macos)
    * [Minikube](#minikube)
    * [kubectl](#kubectl)
    * [Helm](#helm)
  * [Development](#development)
    * [Start minikube](#start-minikube)
    * [Build docker images and deploy all the components](#build-docker-images-and-deploy-all-the-components)
    * [Deploying to GKE or similar K8s test cluster](#deploying-to-gke-or-similar-k8s-test-cluster)
    * [Deploying to a Persistent Cluster](#deploying-to-a-persistent-cluster)

## Run LangStream server
To run LangStream, you need to the following components:
- Kubernetes Cluster (Minikube, AWS EKS, Azure AKS, Google GKE, etc.)
- Apache Kafka or Apache Pulsar cluster
- S3 bucket or API-compatible storage (ex Minio)

For local application development, Minikube is recommended. For information on setting up a Minikube 
environment see [Install Kubernetes Environment on MacOS](#install-kubernetes-environment-on-macos).

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

> **Warning**
> CLI requires Java 11+ to be already installed on your machine.

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
Installing directly the binary will enable auto-completion for the CLI. 

If you installed the CLI with Homebrew, you can enable auto-completion with the following command:
- ZSH

```
[[ $(grep 'langstream generate-completion' "$HOME/.zshrc") ]] || echo -e "source <(langstream generate-completion)" >> "$HOME/.zshrc"
source $HOME/.zshrc # or open another terminal
```
- Bash

```
[[ $(grep 'langstream generate-completion' "$HOME/.bashrc") ]] || echo -e "source <(langstream generate-completion)" >> "$HOME/.bashrc"
source $HOME/.bashrc # or open another terminal
```

### Usage
To get started, run `langstream --help` to see the available commands.
By default, the CLI will connect to the control plane running on `localhost:8090`. 

To configure a different LangStream environment, you can configure a new profile:
```
langstream profiles create dev --web-service-url https://langstream-control-plane --api-gateway-url wss://langstream-api-gateway --tenant my-tenant
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

1. Export two environment variables containing the OpenAI URL and access key:
```
export OPEN_AI_URL=xx
export OPEN_AI_ACCESS_KEY=xx
export OPEN_AI_EMBEDDINGS_MODEL=xxx
export OPEN_AI_CHAT_COMPLETIONS_MODEL=xxx
export OPEN_AI_PROVIDER=openai
```

if you are using Azure Open AI then set OPEN_AI_PROVIDER to azure
```
export OPEN_AI_PROVIDER=azure
```

The values for OPEN_AI_EMBEDDINGS_MODEL and OPEN_AI_CHAT_COMPLETIONS_MODEL depend on your OpenAI environment.
On Azure they must match the names of the deployments you created in the Azure portal.

The [secrets.yaml](./examples/secrets/secrets.yaml) file contains many placeholders that refer to those environment variables.
You can either export them or replace them with the actual values.


2. Deploy the `openai-completions` application

```
./bin/langstream apps deploy openai-completions -app examples/applications/openai-completions -i examples/instances/kafka-kubernetes.yaml -s examples/secrets/secrets.yaml
./bin/langstream apps get openai-completions
```

Check your k8s cluster with `k9s -A` or run `./bin/langstream apps get openai-completions` until the app is deployed.

Test the AI completion using the API gateway. The LangStream CLI provides a convenient chat command to test the application:

```
./bin/langstream gateway chat openai-completions -cg consume-output -pg produce-input -p sessionId=$(uuidgen)
```

## Install Kubernetes Environment on MacOS

### Minikube

To install Minikube on MacOS:

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
## Development

Requirements:
- Minikube
- kubectl
- Helm
- Docker
- Java 17

### Start minikube
  
```
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
