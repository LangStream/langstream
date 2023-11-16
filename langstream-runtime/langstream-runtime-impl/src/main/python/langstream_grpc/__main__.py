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

import asyncio
import logging
import sys

from langstream_grpc.grpc_service import AgentServer


async def main(target, config, context):
    server = AgentServer(target)
    await server.init(config, context)
    await server.start()
    await server.grpc_server.wait_for_termination()
    await server.stop()


if __name__ == "__main__":
    logging.addLevelName(logging.WARNING, "WARN")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d [%(threadName)s] %(levelname)-5s %(name).36s -- %(message)s",  # noqa: E501
        datefmt="%H:%M:%S",
    )

    if len(sys.argv) != 4:
        print("Missing gRPC target or config or agent context")
        print(
            "usage: python -m langstream_grpc <target> <config> <agent_context_config>"
        )
        sys.exit(1)

    asyncio.run(main(*sys.argv[1:]))
