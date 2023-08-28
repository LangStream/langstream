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

set -e
local_k8s_cert=/var/run/secrets/kubernetes.io/serviceaccount/ca.crt
if [ -f $local_k8s_cert ]; then
  java_home=$(dirname $(dirname $(readlink -f $(which java))))
  java_keystore=$java_home/lib/security/cacerts
  keystore=$(realpath /tmp/langstream-keystore.jks)
  keytool -import -trustcacerts -noprompt -alias kubernetes -file $local_k8s_cert -keystore $keystore -storepass langstream
  echo "Keystore created"
  echo "Keystore created, importing local Kubernetes certificate into $java_keystore"
  keytool -importkeystore -srckeystore $java_keystore -destkeystore $keystore -srcstorepass changeit -deststorepass langstream
  echo "Keystore updated"
  JAVA_OPTS="$JAVA_OPTS -Djavax.net.ssl.trustStore=$keystore -Djavax.net.ssl.trustStorePassword=langstream"
fi

exec java ${JAVA_OPTS} -Djava.security.egd=file:/dev/./urandom -cp /app/resources/:/app/classes/:/app/libs/* "ai.langstream.webservice.LangStreamControlPlaneWebApplication" "$@"
