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
