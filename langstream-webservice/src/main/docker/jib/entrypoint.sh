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
  echo "Importing local Kubernetes certificate"
  truststorepath=$(realpath /tmp/truststore.jks)
  keytool -import -trustcacerts -noprompt -alias kubernetes -file $local_k8s_cert -keystore $truststorepath -storepass langstream
  echo "Certificate imported"
  JAVA_OPTS="$JAVA_OPTS -Djavax.net.ssl.trustStore=$truststorepath -Djavax.net.ssl.trustStorePassword=langstream"
fi

exec java ${JAVA_OPTS} -Djava.security.egd=file:/dev/./urandom -cp /app/resources/:/app/classes/:/app/libs/* "ai.langstream.webservice.LangStreamControlPlaneWebApplication" "$@"
