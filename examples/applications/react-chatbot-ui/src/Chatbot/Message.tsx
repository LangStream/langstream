import { Box, Typography } from '@mui/material';
import {
  WsMessage,
} from '../hooks/useWebSocket';

interface Props {
  message: WsMessage;
}

const Message = ({ message }: Props): JSX.Element => {
  const fullTimestampMessage = `${
    message.yours ? 'produced on' : 'consumed from'
  } ${message.gateway}`;

  const parsedMessage = (message: string, yours: boolean): string => {
    try {
      return JSON.stringify(JSON.parse(message), null, 2);
    } catch (e) {
      return message;
    }
  };

  return (
    <Box
      sx={[
        {
          marginBottom: 1,
          width: '70%',
          maxWidth: '70%',
        },
        !message.yours && {
          marginLeft: 0,
          marginRight: 'auto',
        },
        message.yours && {
          marginLeft: 'auto',
          marginRight: 0,
        },
      ]}
    >
      <Box
        sx={[
          {
            alignItems: 'flex-start',
            flex: 1,
            flexDirection: 'column',
            justifyContent: 'space-between',
          },
          !message.yours && {
            marginLeft: 2,
            marginRight: 0,
          },
          message.yours && {
            marginLeft: 0,
            marginRight: 2,
          },
        ]}
      >
        <Typography
          color="text.secondary"
          fontSize="caption.fontSize"
          sx={{ fontStyle: 'italic', marginBottom: 1 }}
        >
          {fullTimestampMessage}
        </Typography>
        <Box
          sx={[
            {
              borderRadius: '6px',
              padding: 2,
              whiteSpace: 'normal',
            },
            message.yours && {
              backgroundColor: '#003A6D',
              color: '#F7F7F7',
            },
            !message.yours && {
              backgroundColor: '#BAE6FF',
              color: '#0A0A0A',
            },
          ]}
        >
          {parsedMessage(message.value, message.yours)}
        </Box>
      </Box>
    </Box>
  );
};

export default Message;
