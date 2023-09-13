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

FROM langstream/langstream-runtime-base:latest-dev

ARG DEBIAN_FRONTEND=noninteractive

RUN mkdir /app && chmod g+w /app

ADD maven/Pipfile.lock /app/Pipfile.lock

ENV NLTK_DATA="/app/nltk_data"

# Install python runtime deps
RUN cd /app && pipenv requirements --categories="packages full" > /app/requirements.txt \
     && python3 -m pip install -r /app/requirements.txt \
     && python3 -m nltk.downloader -d /app/nltk_data punkt averaged_perceptron_tagger

ENV PYTHONPATH="$PYTHONPATH:/app/python_libs"

# Add the runtime code at the end. This optimizes docker layers to not depend on artifacts-specific changes.
ADD maven/lib /app/lib
ADD maven/entrypoint.sh /app/entrypoint.sh
ADD maven/langstream /app/python_libs/langstream
ADD maven/langstream_runtime /app/python_libs/langstream_runtime
ADD maven/agents /app/agents

WORKDIR /app

# The UID must be non-zero. Otherwise, it is arbitrary. No logic should rely on its specific value.
USER 10000
ENTRYPOINT ["/app/entrypoint.sh"]

LABEL org.opencontainers.image.source=https://github.com/LangStream/langstream
LABEL org.opencontainers.image.licenses=Apache-2.0