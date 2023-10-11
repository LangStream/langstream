import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { materialOceanic } from 'react-syntax-highlighter/dist/esm/styles/prism';

interface Props {
  language: string;
  value: string;
}

const CodeBlock = ({ language, value }: Props): JSX.Element => {

  return (
    <SyntaxHighlighter language={language} style={materialOceanic}>
      {value}
    </SyntaxHighlighter>
  );
};

export default CodeBlock;
