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
import logging
import time
from enum import Enum
from typing import List, Tuple, Union

from . import topic_connections_registry
from .source_record_tracker import SourceRecordTracker
from .topic_connector import TopicConsumer, TopicProducer, TopicConsumerSource, TopicProducerSink, \
    TopicConsumerWithDLQSource
from ..api import Source, Sink, Processor, Record
from ..util import SingleRecordProcessor


class ErrorsProcessingOutcome(Enum):
    SKIP = 1
    RETRY = 2
    FAIL = 3


class ErrorsHandler(object):
    def __init__(self, configuration):
        self.failures = 0
        self.configuration = configuration or {}
        self.retries = int(self.configuration.get('retries', 0))
        if self.configuration.get('onFailure') == 'skip':
            self.on_failure_action = ErrorsProcessingOutcome.SKIP
        else:
            self.on_failure_action = ErrorsProcessingOutcome.FAIL

    def handle_errors(self, source_record: Record, error) -> ErrorsProcessingOutcome:
        self.failures += 1
        logging.info(f'Handling error {error} for source record {source_record}, '
                     f'errors count {self.failures} (max retries {self.retries})')
        if self.failures >= self.retries:
            return self.on_failure_action
        else:
            return ErrorsProcessingOutcome.RETRY


def run(configuration, agent=None, max_loops=-1):
    logging.info(f"Pod Configuration {configuration}")

    if 'streamingCluster' not in configuration:
        raise ValueError('streamingCluster cannot be null')

    streaming_cluster = configuration['streamingCluster']
    topic_connections_runtime = topic_connections_registry.get_topic_connections_runtime(streaming_cluster)

    agent_id = f"{configuration['agent']['applicationId']}-{configuration['agent']['agentId']}"

    if 'input' in configuration and len(configuration['input']) > 0:
        consumer = topic_connections_runtime.create_topic_consumer(agent_id, streaming_cluster, configuration['input'])
        dlq_producer = topic_connections_runtime.create_dlq_producer(agent_id, streaming_cluster, configuration['input'])
    else:
        consumer = NoopTopicConsumer()
        dlq_producer = None

    if 'output' in configuration and len(configuration['output']) > 0:
        producer = topic_connections_runtime.create_topic_producer(agent_id, streaming_cluster, configuration['output'])
    else:
        producer = NoopTopicProducer()

    if not agent:
        agent = init_agent(configuration)

    if hasattr(agent, 'read'):
        source = agent
    else:
        if dlq_producer:
            source = TopicConsumerWithDLQSource(consumer, dlq_producer)
        else:
            source = TopicConsumerSource(consumer)

    if hasattr(agent, 'write'):
        sink = agent
    else:
        sink = TopicProducerSink(producer)

    if hasattr(agent, 'process'):
        processor = agent
    else:
        processor = NoopProcessor()

    run_main_loop(source, sink, processor, ErrorsHandler(configuration['agent'].get('errorHandlerConfiguration')),
                  max_loops)


def init_agent(configuration):
    agent_config = configuration['agent']['configuration']
    full_class_name = agent_config['className']
    class_name = full_class_name.split('.')[-1]
    module_name = full_class_name[:-len(class_name) - 1]
    module = importlib.import_module(module_name)
    agent = getattr(module, class_name)()
    call_method_if_exists(agent, 'init', agent_config)
    return agent


def call_method_if_exists(klass, method, *args, **kwargs):
    method = getattr(klass, method, None)
    if callable(method):
        method(*args, **kwargs)


def run_main_loop(source: Source, sink: Sink, processor: Processor, errors_handler: ErrorsHandler, max_loops: int):
    for component in {source, sink, processor}:
        call_method_if_exists(component, 'start')

    try:
        source_record_tracker = SourceRecordTracker(source)
        sink.set_commit_callback(source_record_tracker)
        while max_loops < 0 or max_loops > 0:
            if max_loops > 0:
                max_loops -= 1
            records = source.read()
            if records and len(records) > 0:
                # in case of permanent FAIL this method will throw an exception
                sink_records = run_processor_agent(processor, records, errors_handler)
                # sinkRecord == null is the SKIP case

                # in this case we do not send the records to the sink
                # and the source has already committed the records
                if sink_records is not None:
                    try:
                        source_record_tracker.track(sink_records)
                        for source_record_and_result in sink_records:
                            if isinstance(source_record_and_result[1], Exception):
                                # commit skipped records
                                source.commit([source_record_and_result[0]])
                            else:
                                sink.write(source_record_and_result[1])
                    except Exception as e:
                        logging.exception("Error while processing records")
                        # raise the error
                        # this way the consumer will not commit the records
                        raise e

    finally:
        for component in {source, sink, processor}:
            call_method_if_exists(component, 'close')


def run_processor_agent(
        processor: Processor,
        source_records: List[Record],
        errors_handler: ErrorsHandler) -> List[Tuple[Record, List[Record]]]:
    records_to_process = source_records
    results_by_record = {}
    trial_number = 0
    while len(records_to_process) > 0:
        trial_number += 1
        logging.info(f'run processor on {len(records_to_process)} records (trial #{trial_number})')
        results = safe_process_records(processor, records_to_process)
        records_to_process = []
        for result in results:
            source_record = result[0]
            processor_result = result[1]
            results_by_record[source_record] = result
            if isinstance(processor_result, Exception):
                action = errors_handler.handle_errors(source_record, processor_result)
                if action == ErrorsProcessingOutcome.SKIP:
                    logging.error(f'Unrecoverable error {processor_result} while processing the records, skipping')
                    results_by_record[source_record] = (source_record, processor_result)
                elif action == ErrorsProcessingOutcome.RETRY:
                    logging.error(f'Retryable error {processor_result} while processing the records, retrying')
                    records_to_process.append(source_record)
                elif action == ErrorsProcessingOutcome.FAIL:
                    logging.error(
                        f'Unrecoverable error {processor_result} while processing some the records, failing')
                    # TODO: replace with custom exception ?
                    raise processor_result

    return [results_by_record[source_record] for source_record in source_records]


def safe_process_records(
        processor: Processor,
        records_to_process: List[Record]) -> List[Tuple[Record, Union[List[Record], Exception]]]:
    try:
        return processor.process(records_to_process)
    except Exception as e:
        return [(record, e) for record in records_to_process]


class NoopTopicConsumer(TopicConsumer):
    def read(self):
        logging.info("Sleeping for 1 second, no records...")
        time.sleep(1)
        return []


class NoopTopicProducer(TopicProducer):

    def write(self, records):
        pass


class NoopProcessor(SingleRecordProcessor):
    def process_record(self, record: Record) -> List[Record]:
        return [record]
