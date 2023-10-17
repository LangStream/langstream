# Bedrock Completions

This sample application shows how to use the AWS Bedrock A2I Jurassic-2 model.

## Configure Bedrock access

```
export BEDROCK_ACCESS_KEY=...
export BEDROCK_SECRET_KEY=...
```

## Deploy the LangStream application
```
langstream docker run test -app https://github.com/LangStream/langstream/examples/applications/bedrock-text-completions -s https://raw.githubusercontent.com/LangStream/langstream/main/examples/secrets/secrets.yaml 
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




