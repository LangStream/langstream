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

# Clean up Minikube
# minikube image load --overwrite doens't work sometimes

eval $(minikube docker-env)
set -x
docker image rm -f langstream/langstream-runtime:latest-dev
docker image rm -f langstream/langstream-deployer:latest-dev
docker image rm -f langstream/langstream-control-plane:latest-dev
docker image rm -f langstream/langstream-api-gateway:latest-dev
docker image rm -f langstream/langstream-cli:latest-dev

