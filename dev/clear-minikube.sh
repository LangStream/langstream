# Clean up Minikube
# minikube image load --overwrite doens't work sometimes

eval $(minikube docker-env)
set -x
docker image rm -f datastax/sga-runtime:latest-dev
docker image rm -f datastax/sga-deployer:latest-dev
docker image rm -f datastax/sga-control-plane:latest-dev
docker image rm -f datastax/sga-api-gateway:latest-dev

