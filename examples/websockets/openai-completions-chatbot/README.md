# Expose the LangStream application to users via UI + JS Websockets

This is an example on how to implement a chatbot UI using LangStream api gateway endpoints to connect to the LangStream application.


## How to run the WebUI

```
mkdir -p /tmp/openai-completions-chatbot
wget https://raw.githubusercontent.com/LangStream/langstream/main/examples/websockets/openai-completions-chatbot/index.html -O /tmp/openai-completions-chatbot/index.html
python -m http.server 8888 -d /tmp/openai-completions-chatbot
```

Open [http://localhost:8888](http://localhost:8888) in your browser.


![ls-js](https://github.com/LangStream/langstream/assets/23314389/008f3686-ae0e-48c9-b0e4-353d47474f24)

