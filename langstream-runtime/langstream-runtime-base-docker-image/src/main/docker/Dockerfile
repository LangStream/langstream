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

FROM ubuntu:22.04

ARG DEBIAN_FRONTEND=noninteractive

# Install some utilities
RUN echo 'Acquire::http::Timeout "30";\nAcquire::ftp::Timeout "30";\nAcquire::Retries "3";' > /etc/apt/apt.conf.d/99timeout_and_retries \
     && apt-get update \
     && apt-get -y dist-upgrade \
     && apt-get -y install --no-install-recommends vim netcat dnsutils less procps net-tools iputils-ping unzip \
                 curl ca-certificates wget apt-transport-https software-properties-common gpg-agent

# Install Python3.11
RUN apt-get update && add-apt-repository ppa:deadsnakes/ppa \
      && apt-get -y install --no-install-recommends python3.11-full \
      && update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1 \
      && python3 -m ensurepip && python3 -m pip install poetry-plugin-export


RUN apt-get update \
     && apt-get -y install openjdk-17-jdk

    # Install utility packages for Python unstructured data processing
RUN apt-get update && apt-get -y install --no-install-recommends libmagic-dev poppler-utils tesseract-ocr libreoffice pandoc

# Cleanup apt
RUN  apt-get -y --purge autoremove \
     && apt-get autoclean \
     && apt-get clean \
     && rm -rf /var/lib/apt/lists/*

LABEL org.opencontainers.image.source=https://github.com/LangStream/langstream
LABEL org.opencontainers.image.licenses=Apache-2.0