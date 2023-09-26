# OpenAI Instruct Completions

This sample application shows how to use the `text-bison` model.

## Configure VertexAI

```
export VERTEX_AI_PROJECT=...
export VERTEX_AI_TOKEN=$(gcloud auth print-access-token)
```

## Deploy the LangStream application
```
langstream docker run test -app https://github.com/LangStream/langstream/examples/applications/vertexai-text-completions -s https://raw.githubusercontent.com/LangStream/langstream/main/examples/secrets/secrets.yaml 
```

## Chat with the model

```
./bin/langstream gateway chat test -g chat
```

This model is optimized to run tasks. For example, you can ask it to translate a document into another language.

```
You: 
> Translate "How are you?" in Italian
```




