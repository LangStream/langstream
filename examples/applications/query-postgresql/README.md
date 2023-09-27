# Querying a PostGre Database

This sample application shows how to perform queries against a JDBC database using the PostGre JDBC driver.

## Prerequisites


Install PostGreSQL https://bitnami.com/stack/postgresql/helm


`helm install postgresql oci://registry-1.docker.io/bitnamicharts/postgresql`


## Instructions to access the database:

PostgreSQL can be accessed via port 5432 on the following DNS names from within your cluster:

    postgresql.default.svc.cluster.local - Read/Write connectionImplementation

To get the password for "postgres" run:

    export POSTGRES_PASSWORD=$(kubectl get secret --namespace default postgresql -o jsonpath="{.data.postgres-password}" | base64 -d)

To connect to your database run the following command:

    kubectl run postgresql-client --rm --tty -i --restart='Never' --namespace default --image docker.io/bitnami/postgresql:15.3.0-debian-11-r24 --env="PGPASSWORD=$POSTGRES_PASSWORD" \
      --command -- psql --host postgresql -U postgres -d postgres -p 5432

    > NOTE: If you access the container using bash, make sure that you execute "/opt/bitnami/scripts/postgresql/entrypoint.sh /bin/bash" in order to avoid the error "psql: local user with ID 1001} does not exist"

To connect to your database from outside the cluster execute the following commands:

    kubectl port-forward --namespace default svc/postgresql 5432:5432 &
    PGPASSWORD="$POSTGRES_PASSWORD" psql --host 127.0.0.1 -U postgres -d postgres -p 5432


## Getting the password

In the POSTGRES_PASSWORD env variable you will find the password for the "postgres" user. You can use it to access the database from inside the cluster.
Put it into the configuration.yaml file.

## Create a "products" table

Enter the PG shell:

`kubectl run postgresql-client --rm --tty -i --restart='Never' --namespace default --image docker.io/bitnami/postgresql:15.3.0-debian-11-r24 --env="PGPASSWORD=$POSTGRES_PASSWORD" \
--command -- psql --host postgresql -U postgres -d postgres -p 5432`

Create the table:

`create table products(id INTEGER primary key, name TEXT, description TEXT);`

Insert some data:
`insert into products values(1,'Basic product','This is a very nice product!');`

## Deploy the LangStream application

```
./bin/langstream apps deploy test -app examples/applications/query-postgresql -i examples/instances/kafka-kubernetes.yaml -s examples/secrets/secrets.yaml
```

## Talk with the Chat bot using the CLI
Since the application opens a gateway, we can use the gateway API to send and consume messages.

```
./bin/langstream gateway chat test -cg consume-output -pg produce-input -p sessionId=$(uuidgen)
```

Insert a JSON with "id", "name" and "description":

```
{"id": 1, "name": "test", "description": "test"}
```

