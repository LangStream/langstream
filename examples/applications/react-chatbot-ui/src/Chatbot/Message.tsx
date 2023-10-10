import { Box, Typography } from '@mui/material';
import Markdown from 'react-markdown';
import rehypeKatex from 'rehype-katex'
import remarkGfm from 'remark-gfm';
import remarkMath from 'remark-math';
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
              px: 2,
              py: 1,
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
          <Markdown
            remarkPlugins={[remarkGfm, remarkMath]}
            rehypePlugins={[rehypeKatex]}
          >
            {message.value}
          </Markdown>
        </Box>
      </Box>
    </Box>
  );
};

export default Message;
