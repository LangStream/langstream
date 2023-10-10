import './App.css';
import Chatbot from './Chatbot';
import Header from './Header';

const App = (): JSX.Element => {
  return (
    <div className="App">
      <Header />
      <Chatbot />
    </div>
  );
}

export default App;
