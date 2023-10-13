# Computing text embeddings with Hugging Face - local execution

This sample application shows how to use Hugging Face to compute text embeddings without calling the API.

## Configuring the model

In order to use an open source model, you need to configure the model in the pipeline.yaml file.
You can do it using environment variables or by editing the file directly.

```
export HUGGING_FACE_PROVIDER=local
export HUGGING_FACE_EMBEDDINGS_MODEL=multilingual-e5-small
export HUGGING_FACE_EMBEDDINGS_MODEL_URL=djl://ai.djl.huggingface.pytorch/intfloat/multilingual-e5-small
```

if you want to the API set  HUGGING_FACE_PROVIDER to "api" and configure your API access key.
```
export HUGGING_FACE_PROVIDER=api
export HUGGING_FACE_ACCESS_KEY=your_access_key
export HUGGING_FACE_EMBEDDINGS_MODEL=multilingual-e5-small
```

## Deploy the LangStream application

```
./bin/langstream apps deploy test -app examples/applications/compute-hugging-face-embeddings -i examples/instances/kafka-kubernetes.yaml -s examples/secrets/secrets.yaml
```

## Talk with the Chat bot using the CLI
Since the application opens a gateway, we can use the gateway API to send and consume messages.

```
./bin/langstream gateway chat test -cg bot-output -pg user-input -p sessionId=$(uuidgen)
```

