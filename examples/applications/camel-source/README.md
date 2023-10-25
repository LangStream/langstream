# Using Apache Camel as a source

This sample application shows how to use Apache Camel as a source.
We are using the camel-github component to read the messages from GitHub.

https://camel.apache.org/components/4.0.x/github-component.html

In the application we are using the following LangStream agents:

- camel-source: to read the messages from GitHub
- compute: to manipulate the messages coming from Camel

## Deploy the LangStream application

In order to consume events from GitHub you have to provide a token and the references to the repository.


```
export CAMEL_GITHUB_OAUTH_TOKEN=xxxx
export CAMEL_GITHUB_REPO_NAME=langstream
export CAMEL_GITHUB_REPO_OWNER=langstream
export CAMEL_GITHUB_BRANCH=main
```

Then you can run the application

```
./bin/langstream docker run test -app examples/applications/camel-source -s examples/secrets/secrets.yaml
```

## Chat

Since the application opens a gateway, we can use the gateway API to read the messages:
```
./bin/langstream gateway consume test -c github
```

