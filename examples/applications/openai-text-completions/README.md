# OpenAI Instruct Completions

This sample application shows how to use the `gpt-3.5-turbo-instruct` Open AI model.

## Configure you OpenAI


```
export OPEN_AI_ACCESS_KEY=...
```

## Deploy the LangStream application
```
langstream docker run test -app examples/applications/openai-text-completions -s examples/secrets/secrets.yaml 
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




