import { useEffect, useState } from "react";
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

const Chatbot = (): JSX.Element => {

  const { connect, isConnected, messages, sendMessage } = useWebSockets();
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
      <Box sx={{ flexGrow: 1, overflow: 'auto' }}>
        {messages.map((message, index) => (
          <Message key={index} message={message} />
        ))}
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