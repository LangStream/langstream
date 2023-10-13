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

import { useEffect, useRef, useState } from "react";
import useWebSockets, { ConnectionType } from "../hooks/useWebSocket";
import {
  WEBSOCKET_URL,
  APP_NAME,
  TENANT,
  CONSUMER,
  PRODUCER,
  CREDENTIALS
} from "../config";
import { Box, IconButton, InputAdornment, TextField } from "@mui/material";
import SendIcon from '@mui/icons-material/Send';
import Message from "./Message";
import LoadingMessage from "./LoadingMessage";

const Chatbot = (): JSX.Element => {
  const messagesEl = useRef<HTMLDivElement>(null);
  const { connect, isConnected, messages, sendMessage, waitingForMessage } = useWebSockets();
  const [userInput, setUserInput] = useState<string>('');
  const sessionId = crypto.randomUUID();

  const connectWs = () => {
    connect({
      consumer: {
        baseUrl: WEBSOCKET_URL,
        appName: APP_NAME,
        tenant: TENANT,
        gateway: CONSUMER,
        credentials: CREDENTIALS,
        type: ConnectionType.Consumer,
        sessionId
      },
      producer: { 
        baseUrl: WEBSOCKET_URL,
        appName: APP_NAME,
        tenant: TENANT,
        gateway: PRODUCER, 
        credentials: CREDENTIALS,
        type: ConnectionType.Producer,
        sessionId
      },
    });
  }

  useEffect(() => {
    if (!isConnected) {
      connectWs();
    }
  }, [isConnected]);

  // If we have new messages, make sure that the messages div is
  // showing the latest messages by scrolling to the bottom.
  useEffect(() => {
    if (messagesEl.current) {
      messagesEl.current.scrollTop = messagesEl.current.scrollHeight;
    }
  }, [messages]);

  const handleSendMessage = () => {
    sendMessage(userInput);
    setUserInput('');
  }

  return (
    <Box sx={{
      flexGrow: 1,
      display: 'flex',
      flexDirection: 'column',
      justifyContent: 'flex-end',
      maxHeight: 'calc(90vh - 16px)',
      maxWidth: '1280px',
      width: '100%',
      overflow: 'auto',
      pb: 2,
    }}>
      <Box ref={messagesEl} sx={{ flexGrow: 1, overflow: 'auto' }}>
        {messages.map((message, index) => (
          <Message key={index} message={message} />
        ))}
        {waitingForMessage && <LoadingMessage />}
      </Box>
      <TextField
        fullWidth 
        id="chatbot-producer-input"
        variant="outlined"
        value={userInput}
        onChange={(e) => setUserInput(e.target.value)}
        onKeyDown={(e) => {
          if (e.key === 'Enter' && e.shiftKey === false) {
            e.preventDefault();
            handleSendMessage();
          }
        }}
        InputProps={{
          endAdornment: (
            <InputAdornment position="end">
              <IconButton
                aria-label="send"
                onClick={() => handleSendMessage()}
              >
                <SendIcon />
              </IconButton>
            </InputAdornment>
          )
        }}
        multiline
      />
    </Box>
  )
}

export default Chatbot;