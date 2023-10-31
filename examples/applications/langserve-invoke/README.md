# Invoking a LangServe service

This sample application explains how to invoke a LangServe service and leverage streaming capabilities.

## Set up your LangServe environment

Start you LangServe application, the example below is using the LangServe sample [application](https://github.com/langchain-ai/langserve)

```python
#!/usr/bin/env python
from fastapi import FastAPI
from langchain.prompts import ChatPromptTemplate
from langchain.chat_models import ChatAnthropic, ChatOpenAI
from langserve import add_routes


app = FastAPI(
  title="LangChain Server",
  version="1.0",
  description="A simple api server using Langchain's Runnable interfaces",
)

model = ChatOpenAI()
prompt = ChatPromptTemplate.from_template("tell me a joke about {topic}")
add_routes(
    app,
    prompt | model,
    path="/chain",
)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="localhost", port=8000)
```

## Configure you OpenAI API Key and run the application

```bash
export OPENAI_API_KEY=...
pip install fastapi langserve langchain openai sse_starlette uvicorn
python example.py
```

The sample application is exposing a chain at http://localhost:8000/chain/stream and http://localhost:8000/chain/invoke.

The application, running in docker, connects to http://host.docker.internal:8000/chain/stream

LangStream sends an input like this:

```json
{
    "input": {
        "topic": "cats"
    }
}
```

When "topic" is the topic of the joke you want to generate and it is taken from the user input.

## Deploy the LangStream application
```
./bin/langstream docker run test -app examples/applications/langserve-invoke
```

## Interact with the application

You can now interact with the application using the UI opening your browser at http://localhost:8092/
