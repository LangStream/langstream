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

CLIENT_ID=$1
CLIENT_SECRET=$2
CODE=$3
set -x
URL="https://github.com/login/oauth/access_token"
curl -v -X POST $URL -H "Accept: application/json" -H 'Content-Type: application/x-www-form-urlencoded' -d "code=$CODE&client_id=$CLIENT_ID&client_secret=$CLIENT_SECRET"
