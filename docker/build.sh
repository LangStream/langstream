#!/bin/bash
set -e

./mvnw clean package jib:dockerBuild -pl webservice -am -DskipTests
echo "Image built successfully: datastax/sga:latest"
