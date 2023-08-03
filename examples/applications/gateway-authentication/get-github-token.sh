CLIENT_ID=$1
CLIENT_SECRET=$2
CODE=$3
set -x
URL="https://github.com/login/oauth/access_token"
curl -v -X POST $URL -H "Accept: application/json" -H 'Content-Type: application/x-www-form-urlencoded' -d "code=$CODE&client_id=$CLIENT_ID&client_secret=$CLIENT_SECRET"
