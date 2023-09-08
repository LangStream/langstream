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
```

Deploy the application

```
./bin/langstream apps deploy test -app examples/applications/gateway-authentication -i examples/instances/kafka-kubernetes.yaml -s -s examples/secrets/secrets.yaml
```

## Start the echo chat-bot without authentication
Since the application opens a gateway, we can use the gateway API to send and consume messages.

You have to pass a sessionId to the gateway, it will be used to identify the user.

```
./bin/langstream gateway chat test -cg consume-output-no-auth -pg produce-input-no-auth -p sessionId=$(uuidgen)
```

## Get the user credentials using Google Sign In

The application uses the [Sign In with Google](https://developers.google.com/identity/gsi/web/guides/overview) feature.
You have to get a token from Google and pass it to the gateway.

The easiest way it to configure a button in your application that uses the Google Sign In feature.
https://developers.google.com/identity/gsi/web/guides/display-button?hl=it

You have to get a Client ID from Google (it is something like 99840107278-4363876v0hker43roujaubqom5g07or8.apps.googleusercontent.com)
and configure it in the application.

Set as allowed Javascript origin this url `http://localhost`.

Then you can run the web application with the button 

cd examples/applications/gateway-authentication
python3 -m http.server 80

Then open your browser to http://localhost and click on the button.

You will get a JWT token.


## Start the echo chat-bot with Google authentication
Since the application opens a gateway, we can use the gateway API to send and consume messages.

In this case there's no need to pass a sessionId since the session will be per-user by default,
it depends on your application.

```
google_token=the-token-you-got-above
./bin/langstream gateway chat test -cg consume-output-auth-google -pg produce-input-auth-google -p sessionId=$(uuidgen) -c "$google_token"
```

## Setting up GitHub authentication

Reference: https://docs.github.com/en/apps/creating-github-apps/writing-code-for-a-github-app/building-a-login-with-github-button-with-a-github-app

First of all you have to create a GitHub App:
https://docs.github.com/en/apps/creating-github-apps/registering-a-github-app/registering-a-github-app

Set as redirect page http://localhost/login-github.html

Note your Client ID and your client secret.

You have to put the client-id into the secrets.yaml file.
The Client Secret is not used by the gateway but you need it in order to generate the token.

```
export GITHUB_CLIENT_ID=xxx
```

Deploy the application

```
./bin/langstream apps deploy test -app examples/applications/gateway-authentication -i examples/instances/kafka-kubernetes.yaml -s examples/secrets/secrets.yaml
```

## Get the GitHub token

Start a simple web server
cd examples/applications/gateway-authentication
python3 -m http.server 80

Open the browser to http://localhost/login-github.html

Click on the button and you will get a code.
Then you can fill in the form with the client id and the secret and you will have a token.

## Start the echo chat-bot with GitHub authentication
```
github_token=the-token-you-got-above
./bin/langstream gateway chat test -cg consume-output-auth-github -pg produce-input-auth-github -p sessionId=$(uuidgen) -c "$github_token"
```

