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

#/bin/bash

HERE=$(dirname $0)
docker rm -f prometheus
docker run -d -p 9090:9090 --name prometheus -v $HERE/prometheus.yml:/etc/prometheus/prometheus.yml prom/prometheus
docker rm -f grafana

docker run -d -p 3000:3000 --name=grafana \
  -e "GF_SECURITY_ADMIN_USER=admin" \
  -e "GF_SECURITY_ADMIN_PASSWORD=admin" \
  -v $HERE/provisioning:/etc/grafana/provisioning \
  -v $HERE/dashboards:/var/lib/grafana/dashboards \
  grafana/grafana


echo "Open Grafana at http://localhost:3000/"
echo "Username: admin"
echo "Password: admin"
