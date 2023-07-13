# streaming-gen-ai
Streaming Gen AI Project

# Quick Start

## Run in local Kubernetes (minikube)

```
brew install minikube
minikube start
```

Build docker images in the minikube environment

```
eval $(minikube docker-env)
./docker/build.sh
```

Deploy the control plane and the operator:

```
helm install sga helm/sga --values helm/examples/local.yaml
```

Port forward control plane to localhost:
```
kubectl wait deployment/sga-control-plane --for condition=available --timeout=60s
kubectl port-forward svc/sga-control-plane 8090:8090 &
```

Create a sample app using the CLI:
```
./bin/sga-cli tenants put test
./bin/sga-cli configure test
./bin/sga-cli apps list

TENANT=xx
NAMESPACE=xx
TOKEN=xxx

echo """
secrets:
  - name: astra-token
    id: astra-token
    data:
      tenant: $TENANT 
      namespace: $NAMESPACE
      token: $TOKEN
""" > /tmp/secrets.yaml

./bin/sga-cli apps deploy test -app examples/applications/app2 -i examples/instances/astra-streaming-instance.yaml -s /tmp/secrets.yaml 
./bin/sga-cli apps get test
```






