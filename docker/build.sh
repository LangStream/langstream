#!/bin/bash
set -e

mvnd clean package jib:dockerBuild -pl webservice -am -DskipTests
echo "Image built successfully: datastax/sga:latest"
