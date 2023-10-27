#!/bin/bash
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

START_BROKER=${START_BROKER:-true}
if [ "$START_BROKER" = "true" ]; then
  echo "Starting Broker"
  /kafka/bin/kafka-server-start.sh -daemon /kafka/config/kraft/server.properties
fi

START_MINIO=${START_MINIO:-true}
if [ "$START_MINIO" = "true" ]; then
  echo "Starting Minio"
  /minio/minio server /tmp &
fi

START_HERDDB=${START_HERDDB:-true}
if [ "$START_HERDDB" = "true" ]; then
  echo "Starting Herddb"
  /herddb/herddb/bin/service server start
fi

exec java ${JAVA_OPTS} -Dlangstream.nar.closeClassloaders=false -Dlangstream.development.mode=true -Dlogging.config=/app/logback.xml -Djdk.lang.Process.launchMechanism=vfork -cp "/app/lib/*" "ai.langstream.runtime.tester.Main"
