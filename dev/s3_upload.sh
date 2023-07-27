#!/bin/bash

# Usage: ./minio-upload my-bucket my-file.zip

bucket=$1
file=$2

host=localhost
url=http://localhost:9000
s3_key=minioadmin
s3_secret=minioadmin

fileWithoutPath=$(basename $file)
resource="/${bucket}/${fileWithoutPath}"
content_type="application/octet-stream"
date=`date -R`
_signature="PUT\n\n${content_type}\n${date}\n${resource}"
signature=`echo -en ${_signature} | openssl sha1 -hmac ${s3_secret} -binary | base64`
set -x
curl -X PUT -T "${file}" \
          -H "Host: ${host}" \
          -H "Date: ${date}" \
          -H "Content-Type: ${content_type}" \
          -H "Authorization: AWS ${s3_key}:${signature}" \
          ${url}${resource}
