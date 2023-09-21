# mini-langstream

## Get started

### MacOS/Linux/WSL

```
curl -Ls "https://raw.githubusercontent.com/LangStream/langstream/main/bin/mini-langstream/get-mini-langstream.sh" | bash
```

## Start the cluster

```
$ mini-langstream start
```

## Use the CLI to deploy an application

```
$ mini-langstream cli apps deploy app -i \$(mini-langstream get-instance)" -s https://raw.githubusercontent.com/LangStream/langstream/main/examples/secrets/secrets.yaml -app https://github.com/LangStream/langstream/tree/main/examples/applications/python-processor-exclamation
```

## Start the cluster

```
$ mini-langstream start
```


## Stop the cluster

```
$ mini-langstream delete
```

## Get help

```
mini-langstream help
```
