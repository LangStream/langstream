# Http Request processor

This sample application shows how to execute the http request processor.


## Deploy the LangStream application
```
./bin/langstream apps deploy test -app examples/applications/http-request-processor -i examples/instances/kafka-kubernetes.yaml 
```

## Chat

Since the application opens a gateway, we can use the gateway API to send and consume messages using the use gateway `chat` feature:
```
./bin/langstream gateway chat test -g chat
```

Ask about a city and you will get the timezone.

