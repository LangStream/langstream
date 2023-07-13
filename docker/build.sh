#!/bin/bash
set -e

./mvnw package -am -DskipTests -Pdocker
docker images | head -n 4
