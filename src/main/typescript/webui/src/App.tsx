import * as React from 'react';
import Container from '@mui/material/Container';
import Typography from '@mui/material/Typography';
import AppBar from '@mui/material/AppBar';
import Toolbar from '@mui/material/Toolbar';
import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import { BrowserRouter, Route, Routes } from 'react-router-dom';
import { Query } from './Query';
import { Materialization } from './Materialization';

export default function App() {
  return (
    <Container style = {{ maxWidth: '100vw', padding: '0'  }}>
      <AppBar position = 'static' style = {{ backgroundColor: '#212121' }}>
        <Toolbar variant = 'dense'>
          <Typography variant = 'h6' component = 'div' sx = {{ flexGrow: 1 }}>
            Sudokube
          </Typography>
          <Box sx = {{ display: { xs: 'none', sm: 'block' } }}>
            <Button key = 'Materialization' sx = {{ color: '#fff' }} href = '/materialization'>Materialization</Button>
            <Button key = 'Query' sx = {{ color: '#fff' }} href = 'query'>Query</Button>
          </Box>
        </Toolbar>
      </AppBar>
      <BrowserRouter>
        <Routes>
          <Route path = '/materialization' element = {<Materialization/>} />
          <Route path = '/query' element = {<Query/>} />
        </Routes>
      </BrowserRouter>
    </Container>
  );
}
