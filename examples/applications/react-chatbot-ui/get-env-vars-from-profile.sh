#!/bin/bash  
  
# Receive YAML output from a pipe  
YAML_OUTPUT=$(cat)  
  
# Extract necessary values  
WEBSOCKET_URL=$(echo "$YAML_OUTPUT" | grep "apiGatewayUrl:" | awk '{print $2}' | tr -d '"')  
TENANT=$(echo "$YAML_OUTPUT" | grep "tenant:" | awk '{print $2}' | tr -d '"')
CREDENTIALS=$(echo "$YAML_OUTPUT" | grep "token:" | awk '{print $2}' | tr -d '"')  
  
# Set environment variables  
export REACT_APP_WEBSOCKET_URL=$WEBSOCKET_URL  
export REACT_APP_TENANT=$TENANT  
export REACT_APP_CREDENTIALS=$CREDENTIALS  
  
# Print environment variables  
echo "export REACT_APP_WEBSOCKET_URL=$REACT_APP_WEBSOCKET_URL"  
echo "export REACT_APP_TENANT=$REACT_APP_TENANT"  
