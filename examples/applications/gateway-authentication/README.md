# Configuring authentication on the gateway

This sample application shows how to configure authentication on the gateway.

It is important when you go in production to enable authentication on the gateway to prevent unauthorized access to your application.
A gateway exposes a topic to the public Internet, so it is important to protect it.

This application implements a dummy "echo" pipeline, that takes a message as input and returns it as output.

## Configure the authentication on the gateways

Before deploying the application you need to configure the authentication on the gateways.
The gateways.yaml file contains several possible configuration of the gateways.
Uncomment the one you want to use.

Then you have to fill in the secrets.yaml file and provide the required information to validate the credentials
passed by the user.

## Configuring Google Authentication

Follow this guide to create a Google Client ID:
- https://developers.google.com/identity/gsi/web/guides/get-google-api-clientid

If you want to enable Google authentication in the gateways, also set the `GOOGLE_CLIENT_ID` environment variable.

```
export GOOGLE_CLIENT_ID=xxx

echo """
secrets:
  - name: google
    id: google
    data:
      client-id: $GOOGLE_CLIENT_ID
""" > /tmp/secrets.yaml
```
## 
```
./bin/sga-cli apps deploy test -app examples/applications/gateway-authentication -i examples/instances/kafka-kubernetes.yaml -s /tmp/secrets.yaml
```

## Start the echo chat-bot without authentication
Since the application opens a gateway, we can use the gateway API to send and consume messages.

You have to pass a sessionId to the gateway, it will be used to identify the user.

```
./bin/sga-cli gateway chat test -cg consume-output-no-auth -pg produce-input-no-auth -p sessionId=$(uuidgen)
```


## Get the user credentials

The application uses the [Sign In with Google](https://developers.google.com/identity/gsi/web/guides/overview) feature.
You have to get a token from Google and pass it to the gateway.

The easiest way it to configure a button in your application that uses the Google Sign In feature.
https://developers.google.com/identity/gsi/web/guides/display-button?hl=it

You have to get a Client ID from Google and configure it in the application.

Modify the index.html file in this directory and set the Google Client ID.

Set as allowed Javascript origin this url `http://localhost`.

Then you can run the web application with the button 

cd examples/applications/gateway-authentication
python3 -m http.server 80

Then open your browser to http://localhost and click on the button.

You will get a JWT token.


## Start the echo chat-bot with authentication
Since the application opens a gateway, we can use the gateway API to send and consume messages.

In this case there's no need to pass a sessionId since the session will be per-user by default,
it depends on your application.

```
google_token=the-token-you-got-above
./bin/sga-cli gateway chat test -cg consume-output-auth-google -pg produce-input-auth-google -p sessionId=$(uuidgen) -c "$google_token"
```

