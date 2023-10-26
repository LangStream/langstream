///
/// Copyright DataStax, Inc.
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
/// http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///

import { useCallback, useEffect, useRef, useState } from 'react';

export enum ConnectionType {
  Producer = 'producer',
  Consumer = 'consumer',
}

export enum ReaderMode {
  Earliest = 'earliest',
  Latest = 'latest',
}

export interface ConnectionParams {
  baseUrl: string;
  type: ConnectionType;
  tenant: string;
  appName: string;
  gateway: string;
  sessionId: number | string;
  credentials?: string;
  position?: ReaderMode;
}

export interface WsMessage {
  id: number | string;
  value: string;
  yours: boolean;
  gateway: string;
  key?: string;
  properties?: string;
  headers?: number;
}

interface ConnectionDetails {
  connectionError: boolean;
  hasTimedOut: boolean;
  manuallyDisconnected: boolean;
  isPaused: boolean;
  isTopicReader: boolean;
  lastMessageId?: number | string;
}

const defaultConnectionDetails = {
  connectionError: false,
  hasTimedOut: false,
  isPaused: false,
  isTopicReader: false,
  manuallyDisconnected: false,
};

export const decodeWsMessage = (message: string) => {
  const base64Decoded = window.atob(message);
  let decoded;
  try {
    decoded = decodeURIComponent(base64Decoded);
  } catch {
    return base64Decoded;
  }
  return decoded;
};

const makeWebsocketPath = ({
  baseUrl,
  tenant,
  appName,
  gateway,
  type,
  sessionId,
}: ConnectionParams) => {
  const url = `${baseUrl}/v1/${type === ConnectionType.Consumer ? 'consume': 'produce'}/${tenant}/${appName}/${gateway}?param:sessionId=${sessionId}`;
  return url.replace(/([^:]\/)\/+/g, "$1");
};


export const makeWebsocketUrl = (params: ConnectionParams) => {
  const url = new URL(makeWebsocketPath(params));
  if (params.credentials) {
    url.searchParams.append('credentials', params.credentials);
  }
  return url.toString();
};

const useWebSockets = () => {
  const [producerConnectionParams, setProducerConnectionParams] = useState<
    ConnectionParams | undefined
  >();
  const [consumerConnectionParams, setConsumerConnectionParams] = useState<
    ConnectionParams | undefined
  >();
  const [connectionDetails, setConnectionDetails] = useState<ConnectionDetails>(
    defaultConnectionDetails
  );
  const consumerWs = useRef<WebSocket>();
  const producerWs = useRef<WebSocket>();
  const [messages, setMessages] = useState<WsMessage[]>([]);
  const [waitingForMessage, setWaitingForMessage] = useState<boolean>(false);
  const nextId = useRef<number>(1);

  useEffect(() => {
    nextId.current += 1;
  }, [messages, nextId]);

  const isConnected =
    consumerWs.current?.readyState === WebSocket.OPEN &&
    producerWs.current?.readyState === WebSocket.OPEN;

  const connect = ({
    producer,
    consumer,
  }: {
    producer: ConnectionParams;
    consumer: ConnectionParams;
  }) => {
    setConnectionDetails(defaultConnectionDetails);
    setProducerConnectionParams(producer);
    setConsumerConnectionParams(consumer);
  };

  const connectConsumer = (
    consumer: ConnectionParams,
    topicReader?: boolean
  ) => {
    disconnectConsumer();
    setMessages([]);
    if (topicReader) {
      setConnectionDetails({
        ...defaultConnectionDetails,
        isTopicReader: true,
      });
    }
    setConsumerConnectionParams(consumer);
  };

  const connectProducer = (producer: ConnectionParams) => {
    setProducerConnectionParams(producer);
  };

  const disconnectConsumer = useCallback(() => {
    consumerWs.current?.close();
  }, []);

  const disconnectProducer = useCallback(() => {
    producerWs.current?.close();
  }, []);

  const disconnect = (manual: boolean = false) => {
    if (manual) {
      setConnectionDetails(cd => {
        return { ...cd, manuallyDisconnected: true };
      });
    }
    disconnectConsumer();
    disconnectProducer();
    setMessages([]);
  };

  // Setup connections
  useEffect(() => {
    if (!consumerConnectionParams) {
      return;
    }
    if (consumerWs.current?.OPEN) {
      disconnectConsumer();
    }
    consumerWs.current = new WebSocket(
      makeWebsocketUrl(consumerConnectionParams)
    );
    consumerWs.current.onopen = () => {
      setConnectionDetails(cd => {
        return { ...cd, connectionError: false };
      });
    };
    consumerWs.current.onerror = e => {
      setConnectionDetails(cd => {
        return { ...cd, connectionError: true };
      });
      disconnect();
    };
    consumerWs.current.onclose = e => {
      if (!connectionDetails.manuallyDisconnected) {
        setConnectionDetails(cd => {
          return { ...cd, hasTimedOut: true };
        });
      }
    };
    consumerWs.current.onmessage = ({ data }) => {
      const { offset, record } =
        JSON.parse(data);
      const { key, value, headers } = record;
      setWaitingForMessage(false);
      setMessages(m => {
        if (!headers?.["stream-index"] || headers?.["stream-index"] === "1") {
          return [
            ...m,
            {
              id: headers?.["stream-id"] ?? offset,
              key,
              value,
              gateway: consumerConnectionParams.gateway,
              yours: false,
            },
          ];
        }
        return [
          ...m.slice(0, m.length - 1),
          {
            id: headers?.["stream-id"] ?? offset,
            key,
            value: m[m.length - 1].value + value,
            gateway: consumerConnectionParams.gateway,
            yours: false,
          },
        ];
        
      });
      consumerWs.current?.send(JSON.stringify({ offset }));
    };

    return disconnectConsumer;
  }, [consumerConnectionParams, disconnectConsumer, disconnectProducer]);

  useEffect(() => {
    if (!producerConnectionParams) {
      return;
    }
    if (producerWs.current?.OPEN) {
      disconnectProducer();
    }
    producerWs.current = new WebSocket(
      makeWebsocketUrl(producerConnectionParams)
    );
    producerWs.current.onopen = () => {
      setConnectionDetails(cd => {
        return { ...cd, connectionError: false };
      });
    };
    producerWs.current.onerror = e => {
      setConnectionDetails(cd => {
        return { ...cd, connectionError: true };
      });
      disconnect();
    };
    producerWs.current.onclose = e => {
      if (!connectionDetails.manuallyDisconnected) {
        setConnectionDetails(cd => {
          return { ...cd, hasTimedOut: true };
        });
      }
    };
    producerWs.current.onmessage = e => {
      // noop
    };
    return disconnectProducer;
  }, [producerConnectionParams, disconnectConsumer, disconnectProducer]);

  const sendMessage = useCallback(
    (message: any) => {
      if (!producerConnectionParams) {
        return;
      }
      if (producerWs.current?.readyState === WebSocket.OPEN) {
        setMessages(m => [
          ...m,
          {
            id: nextId.current,
            value: message,
            gateway: producerConnectionParams.gateway,
            yours: true,
          },
        ]);
        setWaitingForMessage(true);
        producerWs.current.send(JSON.stringify({ value: message }));
      } else {
        // noop
      }
    },
    [producerConnectionParams]
  );

  return {
    connect,
    connectConsumer,
    connectProducer,
    connectionError: connectionDetails.connectionError,
    disconnect,
    hasTimedOut: connectionDetails.hasTimedOut,
    isConnected,
    isPaused: connectionDetails.isPaused,
    messages,
    sendMessage,
    waitingForMessage
  };
};

export default useWebSockets;
