#!/bin/bash
#
#
# Copyright DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Usage: ./minio-upload my-bucket my-file.zip

host=$1
url=$2
bucket=$3
file=$4

s3_key=minioadmin
s3_secret=minioadmin

# Function to generate a signature
generate_signature() {
    local method=$1
    local content_type=$2
    local date=$3
    local resource=$4

    local string_to_sign="${method}\n\n${content_type}\n${date}\n${resource}"
    echo -en ${string_to_sign} | openssl sha1 -hmac ${s3_secret} -binary | base64
}

# Check if the bucket exists
check_bucket_exists() {
    local check_resource="/${bucket}/"
    local check_date=`date -R`
    local check_signature=$(generate_signature "HEAD" "" "${check_date}" "${check_resource}")

    local status_code=$(curl -s -o /dev/null -w "%{http_code}" -X HEAD \
        -H "Host: ${host}" \
        -H "Date: ${check_date}" \
        -H "Authorization: AWS ${s3_key}:${check_signature}" \
        ${url}${check_resource})

    if [ "$status_code" == "404" ]; then
        return 1 # Bucket does not exist
    else
        return 0 # Bucket exists
    fi
}

# Create the bucket
create_bucket() {
    local create_resource="/${bucket}/"
    local create_date=`date -R`
    local create_signature=$(generate_signature "PUT" "" "${create_date}" "${create_resource}")

    curl -X PUT \
        -H "Host: ${host}" \
        -H "Date: ${create_date}" \
        -H "Authorization: AWS ${s3_key}:${create_signature}" \
        ${url}${create_resource}
}

# Upload the file
upload_file() {
    local file_without_path=$(basename $file)
    local upload_resource="/${bucket}/${file_without_path}"
    local content_type="application/octet-stream"
    local upload_date=`date -R`
    local upload_signature=$(generate_signature "PUT" "${content_type}" "${upload_date}" "${upload_resource}")

    curl -X PUT -T "${file}" \
        -H "Host: ${host}" \
        -H "Date: ${upload_date}" \
        -H "Content-Type: ${content_type}" \
        -H "Authorization: AWS ${s3_key}:${upload_signature}" \
        ${url}${upload_resource}
}

# Main script
if ! check_bucket_exists; then
    echo "Bucket does not exist. Creating bucket: ${bucket}"
    create_bucket
fi

echo "Uploading file: ${file}"
upload_file

