# Using Apache Camel as a source

This sample application shows how to use Apache NIFI as a source.

https://nifi.apache.org/

In the application we are using the following LangStream agents:

- nifi-source: to read the messages from Facebook
- compute: to manipulate the messages coming from Facebook

## Deploy the LangStream application

In order to consume events from GitHub you have to provide a token and the references to the repository.


```
Then you can run the application

```
./bin/langstream docker run test -app examples/applications/nifi-source -s examples/secrets/secrets.yaml
```

## Chat

Since the application opens a gateway, we can use the gateway API to read the messages:
```
./bin/langstream gateway consume test -c facebook
```

