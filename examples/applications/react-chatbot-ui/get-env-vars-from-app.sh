#!/bin/bash

# Receive YAML output from a pipe
YAML_OUTPUT=$(cat)

# Extract necessary values
APP=$(echo "$YAML_OUTPUT" | grep "application-id" | awk '{print $2}' | tr -d '"')

# Extract IDs for CONSUMER and PRODUCER based on the type
CONSUMER=$(echo "$YAML_OUTPUT" | awk '/- id: / {id=$3} /type: "consume"/ {print id}' | tr -d '"')
PRODUCER=$(echo "$YAML_OUTPUT" | awk '/- id: / {id=$3} /type: "produce"/ {print id}' | tr -d '"')

# Set environment variables
export REACT_APP_APP=$APP
export REACT_APP_CONSUMER=$CONSUMER
export REACT_APP_PRODUCER=$PRODUCER

# Print environment variables
echo "export REACT_APP_APP=$REACT_APP_APP"
echo "export REACT_APP_CONSUMER=$REACT_APP_CONSUMER"
echo "export REACT_APP_PRODUCER=$REACT_APP_PRODUCER"
