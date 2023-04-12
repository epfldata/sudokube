import * as React from 'react';
import Container from '@mui/material/Container';
import Grid from '@mui/material/Grid';
import { FilterChip, chipStyle } from './Chips';
import Chip from '@mui/material/Chip';
import FilterAltIcon from '@mui/icons-material/FilterAlt';
import { DataGrid, GridColDef } from '@mui/x-data-grid';
import { Box } from '@mui/material';
import { RootStore, useRootStore } from './RootStore';
import { observer } from 'mobx-react-lite';

export default observer(function Materialization() {
  const columns: GridColDef[] = [
    {
      field: 'country',
      headerName: 'Country (6 bits)',
      flex: 1,
      sortable: false,
      disableColumnMenu: true
    },
    {
      field: 'city',
      headerName: 'City (6 bits)',
      flex: 1,
      sortable: false,
      disableColumnMenu: true
    },
    {
      field: 'year',
      headerName: 'Year (6 bits)',
      flex: 1,
      sortable: false,
      disableColumnMenu: true
    },
    {
      field: 'month',
      headerName: 'Month (5 bits)',
      flex: 1,
      sortable: false,
      disableColumnMenu: true
    },
    {
      field: 'day',
      headerName: 'Day (6 bits)',
      flex: 1,
      sortable: false,
      disableColumnMenu: true
    },
  ];

  const rows = [
    {
      id: 1,
      country: '\u2589\u2589\u2589\u25A2\u25A2\u25A2',
      city: '\u25A2\u25A2\u25A2\u25A2\u25A2\u25A2',
      year: '\u25A2\u25A2\u25A2\u25A2\u25A2\u25A2',
      month: '\u2589\u2589\u2589\u2589\u25A2',
      day: '\u25A2\u25A2\u25A2\u25A2\u25A2\u25A2'
    }
  ];

  const {metadataStore} = useRootStore();
  return (
    <Container style={{ paddingTop: '20px' }}>
      <h1>{metadataStore.testNumber}</h1>
      <Grid container maxHeight='30vh' overflow='scroll' style={{ paddingTop: '1px', paddingBottom: '1px' }}>
        <Grid item xs={6}>
          <FilterChip text='Country / 5–5' onDelete={() => {}} />
          <Chip
            icon = {<FilterAltIcon style = {{ height: '18px' }} />}
            label = 'Add ...'
            onClick = {() => {metadataStore.testToggle(); console.log("Clicked, " + metadataStore.testNumber)}}
            style = {chipStyle}
            variant = 'outlined'
            color = 'primary'
          />
        </Grid>
      </Grid>
      <Box sx = {{ height: '80vh', width: '100%', marginTop: '20px' }}>
        <DataGrid
          rows = {rows}
          columns = {columns}
          checkboxSelection
          disableRowSelectionOnClick
        />
      </Box>
    </Container>
  )
})
