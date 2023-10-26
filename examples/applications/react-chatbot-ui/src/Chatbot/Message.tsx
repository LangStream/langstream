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

import { Box, Typography } from '@mui/material';
import Markdown from 'react-markdown';
import rehypeKatex from 'rehype-katex'
import remarkGfm from 'remark-gfm';
import remarkMath from 'remark-math';
import {
  WsMessage,
} from '../hooks/useWebSocket';
import CodeBlock from '../Components/CodeBlock';

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
          width: '100%',
          maxWidth: '100%',
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
        ]}
      >
        <Typography
          color="text.secondary"
          fontSize="caption.fontSize"
          sx={[
            { fontStyle: 'italic', marginBottom: 1 },
            message.yours && { textAlign: 'right', mr: 1 }
          ]}
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
            components={{
              code({ node, children, ...props }) {
              
                const className = node?.properties?.className as string[];
                const language = className?.[0] ? className[0].split('-')[1] : '';

                console.log(node?.properties)
                console.log(language)

                return language ? (
                 <CodeBlock
                    language={language}
                    value={String(children).replace(/\n$/, '') ?? ''}
                  />
                 ) : (
                  <code {...props}>
                     {children}
                 </code>
                 )
              }
            }}
          >
            {message.value}
          </Markdown>
        </Box>
      </Box>
    </Box>
  );
};

export default Message;
