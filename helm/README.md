# Helm chart for LangStream

The official Helm chart for LangStream is hosted at [https://github.com/LangStream/charts](https://github.com/LangStream/charts).

To import the repository:
```
helm repo add langstream https://langstream.github.io/charts
helm repo update
helm install -n langstream --create-namespace langstream langstream/langstream --values helm/examples/simple.yaml
```
