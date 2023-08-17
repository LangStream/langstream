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

set -x
minikube image load --overwrite datastax/langstream-deployer:latest-dev
minikube image load --overwrite datastax/langstream-control-plane:latest-dev
minikube image load --overwrite datastax/langstream-runtime:latest-dev
minikube image load --overwrite datastax/langstream-api-gateway:latest-dev
minikube image load --overwrite datastax/langstream-cli:latest-dev
