# Helm chart for LangStream

The official Helm chart for LangStream is hosted at [https://github.com/LangStream/charts](https://github.com/LangStream/charts).

## Setup local code storage (optional)
   
LangStream requires an S3 bucket (or S3-compatible store) to work.
   
You can deploy MinIO on your test cluster:
```
kubectl apply -f https://raw.githubusercontent.com/LangStream/langstream/main/helm/examples/minio-dev.yaml
```

**This is only recommended for test/dev environments. For production, it's recommended to use an external S3-compatible storage.**


## Deploy LangStream

Import the repository:
```
helm repo add langstream https://langstream.github.io/charts
helm repo update
```

Deploy LangStream cluster on the `langstream` namespace:

```
S3_ACCESS_KEY=xx
S3_SECRET_KEY=xx

helm install -n langstream --create-namespace langstream langstream/langstream \
 --set codeStorage.type=s3 \
 --set codeStorage.configuration.access-key=$S3_ACCESS_KEY \
 --set codeStorage.configuration.secret-key=$S3_SECRET_KEY
```
