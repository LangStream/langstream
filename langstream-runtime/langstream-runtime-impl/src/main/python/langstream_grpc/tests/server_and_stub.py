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

from typing import Optional

import grpc, json

from langstream_grpc.grpc_service import AgentServer
from langstream_grpc.proto.agent_pb2_grpc import AgentServiceStub


class ServerAndStub(object):
    def __init__(self, class_name, agent_config = {}, context = {}):
        self.config = agent_config.copy()
        self.config["className"] = class_name
        self.context = context
        self.server: Optional[AgentServer] = None
        self.channel: Optional[grpc.Channel] = None
        self.stub: Optional[AgentServiceStub] = None

    def __enter__(self):
        self.server = AgentServer("[::]:0", json.dumps(self.config), json.dumps(self.context))
        self.server.start()
        self.channel = grpc.insecure_channel("localhost:%d" % self.server.port)
        self.stub = AgentServiceStub(channel=self.channel)
        return self

    def __exit__(self, *args):
        self.channel.close()
        self.server.stop()
