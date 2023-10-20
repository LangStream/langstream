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

set -e

version=$1
if [ -z "$version" ]; then
  echo "Usage: $0 <version>"
  exit 1
fi

brew install LangStream/langstream/langstream
langstream -V
langstream -V | grep $version
langstream -h | grep "Usage: langstream"

# pull before otherwise the test will timeout
docker pull ghcr.io/langstream/langstream-runtime-tester:$version

./mvnw -ntp install -pl langstream-e2e-tests -am -DskipTests -Dspotless.skip -Dlicense.skip
./mvnw -ntp verify -pl langstream-e2e-tests -De2eTests -Dgroups="cli"