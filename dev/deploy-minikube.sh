set -x
minikube image load --overwrite datastax/sga-deployer:latest-dev
minikube image load --overwrite datastax/sga-control-plane:latest-dev
minikube image load --overwrite datastax/sga-runtime:latest-dev
minikube image load --overwrite datastax/sga-api-gateway:latest-dev
