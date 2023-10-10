import { Box, IconButton, Link, Typography } from "@mui/material";
import GitHubIcon from '@mui/icons-material/GitHub';
import ImportContactsIcon from '@mui/icons-material/ImportContacts';
import logo from '../assets/logo.png';

const Header = (): JSX.Element => {
  return (
    <Box
      component="header"
      sx={{
        display: "flex",
        justifyContent: "space-between",
        height: "10vh",
        width: "100%",
        mt: 2,
      }}
    >
      <img
        src={logo}
        alt="LangStream Logo"
        style={{ height: "64px", marginLeft: "1rem" }}
      />
      <Typography variant="h1" sx={{ fontSize:"2rem", color: "#062087"}}>
        LangStream Chatbot
      </Typography>
      <Box>
        <Link
          href="https://langstream.ai/"
          target="_blank"
          rel="noopener noreferrer"
          sx={{ color: '#062087'}}
        >
          LangStream.ai
        </Link>
        <IconButton
          onClick={() => window.open('https://docs.langstream.ai/', '_blank', 'noopener,noreferrer')}
          sx={{ color: '#062087'}}
        >
          <ImportContactsIcon />
        </IconButton>
        <IconButton
          onClick={() => window.open('https://github.com/LangStream/langstream', '_blank', 'noopener,noreferrer')}
          sx={{ color: '#062087'}}
        >
          <GitHubIcon />
        </IconButton>
      </Box>
    </Box>
  );
}

export default Header;
