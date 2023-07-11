#!/bin/bash
set -e

# Control plane
./mvnw package jib:dockerBuild -pl webservice -am -DskipTests
echo "Image built successfully: datastax/sga-control-plane:latest-dev"
./mvnw package -pl :k8s-deployer-operator -am -DskipTests
echo "Image built successfully: datastax/sga-deployer:latest-dev"
