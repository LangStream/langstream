## LangStream Examples

This folder contains many examples of how to use LangStream.

The examples are organized by the type of application they are demonstrating.
You can find the 'instances' directory that contain sample configuration to deploy the application on different platforms.
The 'secrets' directory contains sample secrets that can be used to deploy the application.
The 'applications' directory contains the application code.

Most of the applications work with any 'instance' file.

### Deploying an application locally

If you want to deploy your application on a local Kubernetes cluster, you should use the 'instances/kafka-kubernetes.yaml'.


### Deploying an application using DataStax Astra

If you want to deploy your application on DataStax Astra, you should use the 'instances/astra.yaml'.
In this case you are using Kafka topics on an Astra Streaming tenant, and you are using the native LangStream Kubernetes compute engine
to run your application.
With this setup you are able to use any Kafka Connect connectors and write Python agents.

