import * as React from 'react';
import Container from '@mui/material/Container';
import Typography from '@mui/material/Typography';
import AppBar from '@mui/material/AppBar';
import Toolbar from '@mui/material/Toolbar';
import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import { BrowserRouter, Route, Routes } from 'react-router-dom';
import Query from './QueryView';
import Materialization from './MaterializationView';
import { RootStore, RootStoreContextProvider } from './RootStore';
import ExploreTransform from './ExploreTransformView';
import { ErrorPopup } from './ErrorPopup';

export default function App({rootStore}: {rootStore: RootStore}) {
  return (
    <RootStoreContextProvider rootStore = {rootStore}>
      <Container style = {{ maxWidth: '100vw', padding: '0'  }}>
        <AppBar position = 'static' style = {{ backgroundColor: '#212121' }}>
          <Toolbar variant = 'dense'>
            <Typography variant = 'h6' component = 'div' sx = {{ flexGrow: 1 }}>
              Sudokube
            </Typography>
            <Box sx = {{ display: { xs: 'none', sm: 'block' } }}>
              <Button key = 'Materialize' sx = {{ color: '#fff' }} href = '/materialize'>Materialize</Button>
              <Button key = 'Explore' sx = {{ color: '#fff' }} href = '/explore-transform'>Explore/Transform</Button>
              <Button key = 'Query' sx = {{ color: '#fff' }} href = 'query'>Query</Button>
            </Box>
          </Toolbar>
        </AppBar>
        <BrowserRouter>
          <Routes>
            <Route path = '/materialize' element = {<Materialization/>} />
            <Route path = '/query' element = {<Query/>} />
            <Route path = '/explore-transform' element = {<ExploreTransform/>} />
          </Routes>
        </BrowserRouter>
        <ErrorPopup/>
      </Container>
    </RootStoreContextProvider>
  );
}
