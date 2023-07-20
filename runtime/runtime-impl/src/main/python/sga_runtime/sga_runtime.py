import importlib
import logging
import time

from . import topic_connections_registry


def run(configuration, max_loops=-1):
    logging.info(f"Pod Configuration {configuration}")

    if 'streamingCluster' not in configuration:
        raise ValueError('streamingCluster cannot be null')

    streaming_cluster = configuration['streamingCluster']
    topic_connections_runtime = topic_connections_registry.get_topic_connections_runtime(streaming_cluster)

    agent = init_agent(configuration)
    agent_id = f"{configuration['agent']['applicationId']}-{configuration['agent']['agentId']}"

    if hasattr(agent, 'read'):
        source = agent
    else:
        if 'input' in configuration and len(configuration['input']) > 0:
            source = topic_connections_runtime.create_source(agent_id,
                                                             streaming_cluster,
                                                             configuration['input'])
        else:
            source = NoopSource()

    if hasattr(agent, 'write'):
        sink = agent
    else:
        if 'output' in configuration and len(configuration['output']) > 0:
            sink = topic_connections_runtime.create_sink(configuration['agent']['applicationId'], streaming_cluster,
                                                         configuration['output'])
        else:
            sink = NoopSink()

    if hasattr(agent, 'process'):
        processor = agent
    else:
        processor = NoopProcessor()

    run_main_loop(source, sink, processor, max_loops)


def init_agent(configuration):
    agent_config = configuration['agent']['configuration']
    full_class_name = agent_config['className']
    class_name = full_class_name.split('.')[-1]
    module_name = full_class_name[:-len(class_name) - 1]
    module = importlib.import_module(module_name)
    agent = getattr(module, class_name)()
    call_method_if_exists(agent, 'init', config=agent_config)
    return agent


def call_method_if_exists(klass, method, *args, **kwargs):
    method = getattr(klass, method, None)
    if callable(method):
        method(*args, **kwargs)


def run_main_loop(source, sink, function, max_loops):
    for component in {source, sink, function}:
        call_method_if_exists(component, 'start')

    try:
        while max_loops < 0 or max_loops > 0:
            if max_loops > 0:
                max_loops -= 1
            # TODO: handle semantics, transactions...
            records = source.read()
            if records and len(records) > 0:
                try:
                    output_records = function.process(records)
                    if output_records and len(output_records) > 0:
                        sink.write(output_records)
                except Exception:
                    # TODO: handle errors
                    logging.exception("Error while processing records")

                call_method_if_exists(source, 'commit')

    finally:
        for component in {source, sink, function}:
            call_method_if_exists(component, 'close')


class NoopSource(object):
    @staticmethod
    def read():
        logging.info("Sleeping for 1 second, no records...")
        time.sleep(1)
        return []


class NoopSink(object):
    def write(self, records):
        pass


class NoopProcessor(object):
    @staticmethod
    def process(records):
        return records
