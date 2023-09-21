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

import importlib
import json
import logging
import threading
import time
from enum import Enum
from functools import partial
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import List, Tuple, Union, Dict, Any, Optional

from .api import (
    Agent,
    Source,
    Sink,
    Processor,
    Record,
    CommitCallback,
    RecordType,
)
from .util import SingleRecordProcessor, SimpleRecord
from . import topic_connections_registry
from .source_record_tracker import SourceRecordTracker
from .topic_connector import (
    TopicConsumer,
    TopicProducer,
    TopicProducerSink,
    TopicConsumerWithDLQSource,
)

LOG = logging.getLogger(__name__)


def current_time_millis():
    return int(time.time_ns() / 1000_000)


class ErrorsProcessingOutcome(str, Enum):
    SKIP = "SKIP"
    RETRY = "RETRY"
    FAIL = "FAIL"


class ErrorsHandler(object):
    def __init__(self, configuration):
        self.failures = 0
        self.configuration = configuration or {}
        self.retries = int(self.configuration.get("retries", 0))
        self.on_failure_action = self.configuration.get("onFailure", "fail")

    def handle_errors(self, source_record: Record, error) -> ErrorsProcessingOutcome:
        self.failures += 1
        LOG.info(
            f"Handling error {error} for source record {source_record}, "
            f"errors count {self.failures} (max retries {self.retries})"
        )
        if self.failures >= self.retries:
            if self.on_failure_action == "skip":
                return ErrorsProcessingOutcome.SKIP
            else:
                return ErrorsProcessingOutcome.FAIL
        else:
            return ErrorsProcessingOutcome.RETRY

    def fail_processing_on_permanent_errors(self):
        return self.on_failure_action not in ["skip", "dead-letter"]


class ComponentType(str, Enum):
    SOURCE = "SOURCE"
    PROCESSOR = "PROCESSOR"
    SINK = "SINK"


class RuntimeAgent(Agent):
    def __init__(
        self,
        agent: Union[Source, Sink, Processor],
        component_type: ComponentType,
        agent_id,
        agent_type,
        started_at=None,
    ):
        self.agent = agent
        self.component_type = component_type
        self.agent_id = agent_id
        self.agent_type = agent_type
        self.started_at = started_at or current_time_millis()
        self.total_in = 0
        self.total_out = 0
        self.total_errors = 0
        self.last_processed_at = 0

    def init(self, config: Dict[str, Any]):
        call_method_if_exists(self.agent, "init", config)

    def start(self):
        call_method_if_exists(self.agent, "start")

    def close(self):
        call_method_if_exists(self.agent, "close")

    def agent_info(self) -> Dict[str, Any]:
        return call_method_if_exists(self.agent, "agent_info") or {}

    def get_agent_status(self) -> List[Dict[str, Any]]:
        return [
            {
                "agent-id": self.agent_id,
                "agent-type": self.agent_type,
                "component-type": self.component_type,
                "info": self.agent_info(),
                "metrics": {
                    "total-in": self.total_in,
                    "total-out": self.total_out,
                    "started-at": self.started_at,
                    "last-processed-at": self.last_processed_at,
                },
            }
        ]


class RuntimeSource(RuntimeAgent, Source):
    def __init__(self, source: Source, agent_id, agent_type, started_at=None):
        super().__init__(
            source,
            ComponentType.SOURCE,
            agent_id,
            agent_type,
            started_at or current_time_millis(),
        )

    def read(self) -> List[RecordType]:
        read = self.agent.read()
        self.last_processed_at = current_time_millis()
        self.total_out += len(read)
        return read

    def commit(self, records: List[Record]):
        call_method_if_exists(self.agent, "commit", records)

    def permanent_failure(self, record: Record, error: Exception):
        if callable(getattr(self.agent, "permanent_failure", None)):
            self.agent.permanent_failure(record, error)
        else:
            raise error


class RuntimeProcessor(RuntimeAgent, Processor):
    def __init__(
        self,
        processor: Processor,
        agent_id,
        agent_type,
        started_at=None,
    ):
        super().__init__(
            processor,
            ComponentType.PROCESSOR,
            agent_id,
            agent_type,
            started_at or current_time_millis(),
        )

    def process(
        self, records: List[Record]
    ) -> List[Tuple[Record, Union[List[RecordType], Exception]]]:
        self.last_processed_at = current_time_millis()
        self.total_in += len(records)
        results = self.agent.process(records)
        for _, result in results:
            if isinstance(result, Exception):
                self.total_errors += 1
            else:
                self.total_out += len(result)
        return results


class RuntimeSink(RuntimeAgent, Sink):
    def __init__(self, sink: Sink, agent_id, agent_type, started_at=None):
        super().__init__(
            sink,
            ComponentType.SINK,
            agent_id,
            agent_type,
            started_at or current_time_millis(),
        )

    def write(self, records: List[Record]):
        self.last_processed_at = current_time_millis()
        self.total_in += len(records)
        return self.agent.write(records)

    def set_commit_callback(self, commit_callback: CommitCallback):
        self.agent.set_commit_callback(commit_callback)


class AgentInfo(object):
    def __init__(self):
        self.source: Optional[RuntimeSource] = None
        self.processor: Optional[RuntimeProcessor] = None
        self.sink: Optional[RuntimeSink] = None

    def worker_status(self):
        status = []
        for agent in [self.source, self.processor, self.sink]:
            if agent:
                status.extend(agent.get_agent_status())
        return status


# We use a basic HTTP server to not bring a lot of dependencies to the runtime
class HttpHandler(BaseHTTPRequestHandler):
    def __init__(self, agent_info, *args, **kwargs):
        self.agent_info = agent_info
        # BaseHTTPRequestHandler calls do_GET **inside** __init__ !!!
        # So we have to call super().__init__ after setting attributes.
        super().__init__(*args, **kwargs)

    def do_GET(self):
        if self.path == "/info":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(
                json.dumps(self.agent_info.worker_status()).encode("utf-8")
            )
        else:
            self.send_response(404)
            self.end_headers()


def run_with_server(
    configuration, agent=None, agent_info: AgentInfo = AgentInfo(), max_loops=-1
):
    httpd = HTTPServer(("", 8081), partial(HttpHandler, agent_info))
    http_thread = threading.Thread(
        target=lambda s: s.serve_forever(), args=(httpd,), daemon=True
    )
    http_thread.start()

    try:
        run(configuration, agent, agent_info, max_loops)
    finally:
        httpd.shutdown()
        http_thread.join()


def run(configuration, agent=None, agent_info: AgentInfo = AgentInfo(), max_loops=-1):
    LOG.info(f"Pod Configuration {configuration}")

    if "streamingCluster" not in configuration:
        raise ValueError("streamingCluster cannot be null")

    streaming_cluster = configuration["streamingCluster"]
    topic_connections_runtime = (
        topic_connections_registry.get_topic_connections_runtime(streaming_cluster)
    )

    agent_id = configuration["agent"].get("agentId")
    agent_type = configuration["agent"].get("agentType")
    application_agent_id = f"{configuration['agent'].get('applicationId')}-{agent_id}"

    if "input" in configuration and len(configuration["input"]) > 0:
        consumer = topic_connections_runtime.create_topic_consumer(
            application_agent_id, streaming_cluster, configuration["input"]
        )
        dlq_producer = topic_connections_runtime.create_dlq_producer(
            application_agent_id, streaming_cluster, configuration["input"]
        )
    else:
        consumer = NoopTopicConsumer()
        dlq_producer = None

    if "output" in configuration and len(configuration["output"]) > 0:
        producer = topic_connections_runtime.create_topic_producer(
            application_agent_id, streaming_cluster, configuration["output"]
        )
    else:
        producer = NoopTopicProducer()

    if not agent:
        agent = init_agent(configuration)

    if hasattr(agent, "read"):
        source = RuntimeSource(agent, agent_id, agent_type)
    else:
        source = RuntimeSource(
            TopicConsumerWithDLQSource(
                consumer, dlq_producer if dlq_producer else NoopTopicProducer()
            ),
            "topic-source",
            "topic-source",
        )

    agent_info.source = source

    if hasattr(agent, "write"):
        sink = RuntimeSink(agent, agent_id, agent_type)
    else:
        sink = RuntimeSink(TopicProducerSink(producer), "topic-sink", "topic-sink")

    agent_info.sink = sink

    if hasattr(agent, "process"):
        processor = RuntimeProcessor(agent, agent_id, agent_type)
    else:
        processor = RuntimeProcessor(NoopProcessor(), "identity", "identity")

    agent_info.processor = processor

    run_main_loop(
        source,
        sink,
        processor,
        ErrorsHandler(configuration["agent"].get("errorHandlerConfiguration")),
        max_loops,
    )


def init_agent(configuration):
    agent_config = configuration["agent"]["configuration"]
    full_class_name = agent_config["className"]
    class_name = full_class_name.split(".")[-1]
    module_name = full_class_name[: -len(class_name) - 1]
    module = importlib.import_module(module_name)
    agent = getattr(module, class_name)()
    call_method_if_exists(agent, "init", agent_config)
    return agent


def call_method_if_exists(klass, method, *args, **kwargs):
    method = getattr(klass, method, None)
    if callable(method):
        return method(*args, **kwargs)


def wrap_in_record(records):
    if isinstance(records, Exception):
        return records
    for i, record in enumerate(records):
        if isinstance(record, tuple) or isinstance(record, list):
            records[i] = SimpleRecord(*record)
        if isinstance(record, dict):
            records[i] = SimpleRecord(**record)
    return records


def run_main_loop(
    source: RuntimeSource,
    sink: RuntimeSink,
    processor: RuntimeProcessor,
    errors_handler: ErrorsHandler,
    max_loops: int,
):
    for component in {a.agent: a for a in {source, sink, processor}}.values():
        component.start()

    try:
        source_record_tracker = SourceRecordTracker(source)
        sink.set_commit_callback(source_record_tracker)
        while max_loops < 0 or max_loops > 0:
            if max_loops > 0:
                max_loops -= 1
            records = source.read()
            if records and len(records) > 0:
                # in case of permanent FAIL this method will throw an exception
                processor_results = run_processor_agent(
                    processor, wrap_in_record(records), errors_handler, source
                )
                # sinkRecord == null is the SKIP case

                # in this case we do not send the records to the sink
                # and the source has already committed the records
                if processor_results is not None:
                    try:
                        for i, result in enumerate(processor_results):
                            processor_results[i] = (
                                result[0],
                                wrap_in_record(result[1]),
                            )
                        source_record_tracker.track(processor_results)
                        for source_record, processor_result in processor_results:
                            if isinstance(processor_result, Exception):
                                # commit skipped records
                                source.commit([source_record])
                            else:
                                if len(processor_result) > 0:
                                    write_records_to_the_sink(
                                        sink,
                                        source_record,
                                        processor_result,
                                        errors_handler,
                                        source_record_tracker,
                                        source,
                                    )
                    except Exception as e:
                        LOG.exception("Error while processing records")
                        # raise the error
                        # this way the consumer will not commit the records
                        raise e

    finally:
        for component in {a.agent: a for a in {source, sink, processor}}.values():
            component.close()


def run_processor_agent(
    processor: Processor,
    source_records: List[Record],
    errors_handler: ErrorsHandler,
    source: Source,
) -> List[Tuple[Record, List[Record]]]:
    records_to_process = source_records
    results_by_record = {}
    trial_number = 0
    while len(records_to_process) > 0:
        trial_number += 1
        LOG.info(
            f"run processor on {len(records_to_process)} records "
            f"(trial #{trial_number})"
        )
        results = safe_process_records(processor, records_to_process)
        records_to_process = []
        for result in results:
            source_record = result[0]
            processor_result = result[1]
            results_by_record[source_record] = result
            if isinstance(processor_result, Exception):
                action = errors_handler.handle_errors(source_record, processor_result)
                if action == ErrorsProcessingOutcome.SKIP:
                    LOG.error(
                        f"Unrecoverable error {processor_result} while processing the "
                        f"records, skipping"
                    )
                    results_by_record[source_record] = (source_record, processor_result)
                elif action == ErrorsProcessingOutcome.RETRY:
                    LOG.error(
                        f"Retryable error {processor_result} while processing the "
                        f"records, retrying"
                    )
                    records_to_process.append(source_record)
                elif action == ErrorsProcessingOutcome.FAIL:
                    LOG.error(
                        f"Unrecoverable error {processor_result} while processing some "
                        f"records, failing"
                    )
                    # TODO: replace with custom exception ?
                    source.permanent_failure(source_record, processor_result)
                    if errors_handler.fail_processing_on_permanent_errors():
                        LOG.error("Failing processing on permanent error")
                        raise processor_result
                    # in case the source does not throw an exception we mark the record
                    # as "skipped"
                    results_by_record[source_record] = (source_record, processor_result)
                else:
                    raise ValueError(f"Unexpected value: {action}")

    return [results_by_record[source_record] for source_record in source_records]


def safe_process_records(
    processor: Processor, records_to_process: List[Record]
) -> List[Tuple[Record, Union[List[Record], Exception]]]:
    try:
        return processor.process(records_to_process)
    except Exception as e:
        return [(record, e) for record in records_to_process]


def write_records_to_the_sink(
    sink: Sink,
    source_record: Record,
    processor_records: List[Record],
    errors_handler: ErrorsHandler,
    source_record_tracker: SourceRecordTracker,
    source: Source,
):
    for_the_sink = processor_records.copy()

    while True:
        try:
            sink.write(for_the_sink)
            return
        except Exception as error:
            action = errors_handler.handle_errors(source_record, error)
            if action == ErrorsProcessingOutcome.SKIP:
                # skip (the whole batch)
                LOG.error(
                    f"Unrecoverable error {error} while processing the records, "
                    f"skipping"
                )
                source_record_tracker.commit(for_the_sink)
                return
            elif action == ErrorsProcessingOutcome.RETRY:
                # retry (the whole batch)
                LOG.error(
                    f"Retryable error {error} while processing the records, retrying"
                )
            elif action == ErrorsProcessingOutcome.FAIL:
                LOG.error(
                    f"Unrecoverable error {error} while processing some records, "
                    f"failing"
                )
                # TODO: replace with custom exception ?
                source.permanent_failure(source_record, error)
                if errors_handler.fail_processing_on_permanent_errors():
                    LOG.error("Failing processing on permanent error")
                    raise error
                # in case the source does not throw an exception we mark the record as
                # "skipped"
                source_record_tracker.commit(for_the_sink)
            else:
                raise ValueError(f"Unexpected value: {action}")


class NoopTopicConsumer(TopicConsumer):
    def read(self):
        LOG.info("Sleeping for 1 second, no records...")
        time.sleep(1)
        return []


class NoopTopicProducer(TopicProducer):
    def write(self, records):
        pass


class NoopProcessor(SingleRecordProcessor):
    def process_record(self, record: Record) -> List[RecordType]:
        return [record]
