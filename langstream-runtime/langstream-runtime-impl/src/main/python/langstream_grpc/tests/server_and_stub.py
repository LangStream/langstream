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

import grpc
import json

from langstream_grpc.grpc_service import AgentServer
from langstream_grpc.proto.agent_pb2_grpc import AgentServiceStub


class ServerAndStub(object):
    def __init__(self, class_name, agent_config={}, context={}):
        self.config = agent_config.copy()
        self.config["className"] = class_name
        self.context = context
        self.server: Optional[AgentServer] = None
        self.channel: Optional[grpc.aio.Channel] = None
        self.stub: Optional[AgentServiceStub] = None

    async def __aenter__(self):
        self.server = AgentServer("[::]:0")
        await self.server.init(json.dumps(self.config), json.dumps(self.context))
        await self.server.start()
        self.channel = grpc.aio.insecure_channel(
            "localhost:%d" % self.server.port,
            options=[
                ("grpc.max_send_message_length", 0x7FFFFFFF),
                ("grpc.max_receive_message_length", 0x7FFFFFFF),
            ],
        )
        self.stub = AgentServiceStub(channel=self.channel)
        return self

    async def __aexit__(self, *args):
        await self.channel.close()
        await self.server.stop()
