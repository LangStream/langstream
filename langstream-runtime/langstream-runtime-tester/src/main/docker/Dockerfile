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

FROM langstream/langstream-runtime:latest-dev

ARG DEBIAN_FRONTEND=noninteractive
USER 0

ADD maven/minio /minio/minio
ADD maven/herddb-services-0.28.0.zip /herddb/herddb-services-0.28.0.zip
COPY maven/kafka_2.13-3.5.2.tgz /kafka/kafka.tgz

RUN chmod -R g+w /kafka \
       && cd /kafka \
       && tar xvf kafka.tgz --strip-components=1 \
       && export UUID=$(bin/kafka-storage.sh random-uuid) \
       && bin/kafka-storage.sh format -t $UUID -c config/kraft/server.properties \
       && mkdir -p /tmp/kraft-combined-logs \
       && chmod a+rwx /tmp/kraft-combined-logs \
       && rm -rf /kafka/kafka.tgz \
       && chmod a+x /minio/minio \
       && chmod a+x /herddb \
       && cd /herddb \
       && unzip herddb-services-0.28.0.zip \
       && mv herddb-services-0.28.0 herddb \
       && chmod a+x /herddb/herddb \
       && chmod a+x /herddb/herddb/bin/* \
       && rm -rf herddb-services-0.28.0.zip \
       && chown 10000:0 -R /herddb

# Add the runtime code at the end. This optimizes docker layers to not depend on artifacts-specific changes.
RUN rm -rf /app/lib
ADD maven/lib /app/lib
RUN rm -f /app/lib/*netty*
ADD maven/entrypoint.sh /app/entrypoint.sh
ADD maven/logback.xml /app/logback.xml

WORKDIR /app

# The UID must be non-zero. Otherwise, it is arbitrary. No logic should rely on its specific value.
USER 10000
ENTRYPOINT ["/app/entrypoint.sh"]

LABEL org.opencontainers.image.source=https://github.com/LangStream/langstream
LABEL org.opencontainers.image.licenses=Apache-2.0