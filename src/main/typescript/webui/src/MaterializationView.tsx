import * as React from 'react';
import Container from '@mui/material/Container';
import Grid from '@mui/material/Grid';
import { AddCuboidsFilterChip, ButtonChip, FilterChip, chipStyle } from './Chips';
import Chip from '@mui/material/Chip';
import FilterAltIcon from '@mui/icons-material/FilterAlt';
import { DataGrid, GridColDef } from '@mui/x-data-grid';
import { Box, Button, Dialog, DialogActions, DialogContent, DialogTitle, FormControl, InputLabel, MenuItem, Select, TextField } from '@mui/material';
import { observer } from 'mobx-react-lite';
import { useRootStore } from './RootStore';
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

export default observer(function Materialization() {

  const { materializationStore } = useRootStore();
  const dimensions = materializationStore.dimensions;
  return (
    <Container style={{ paddingTop: '20px' }}>
      <SelectDataset/>
      <div style = {{ marginTop: 5, marginBottom: 5 }}>
        <span><b>Add cuboids to materialize </b></span>
        <SpecifyStrategy/>
        <ChooseCuboids/>
      </div>
      <div>
        <div style = {{ marginTop: 5, marginBottom: 10 }}><b>Cuboids chosen</b></div>
        <Grid container maxHeight='30vh' overflow='scroll' style={{ paddingTop: '1px', paddingBottom: '1px' }}>
          <Grid item xs={6}>
            { materializationStore.addCuboidsFilters.map((filter, index) => (
              <FilterChip
                key = { 'materialization-filter-chip-' + dimensions[filter.dimensionIndex].name + '-' + filter.bitsFrom + '-' + filter.bitsTo }
                text = { dimensions[filter.dimensionIndex].name + ' / ' + filter.bitsFrom + '–' + filter.bitsTo }
                onDelete = { () => materializationStore.removeFilterAtIndex(index) }
              /> 
            )) }
            <Chip
              icon = {<FilterAltIcon style = {{ height: '18px' }} />}
              label = 'Add ...'
              onClick = {() => {}}
              style = {chipStyle}
              variant = 'outlined'
              color = 'primary'
            />
          </Grid>
        </Grid>
        <Box sx = {{ height: '65vh', width: '100%', marginTop: '20px' }}>
          <DataGrid
            rows = {rows}
            columns = {columns}
            checkboxSelection
            disableRowSelectionOnClick
          />
        </Box>
      </div>
      <Button style = {{ marginTop: 10 }}>Materialize</Button>
    </Container>
  )
})

const SelectDataset = observer(() => {
  const { materializationStore } = useRootStore();
  return ( <div>
    <FormControl sx={{ minWidth: 200 }}>
      <InputLabel htmlFor="select-dataset">Dataset</InputLabel>
      <Select
        id="select-dataset" label="Dataset"
        style = {{ maxHeight: '50px', marginBottom: '5px' }}
        value = {materializationStore.dataset}
        onChange = { e => materializationStore.setDatasetAndLoadDimensions(e.target.value) }>
        { materializationStore.datasets.map((dataset, index) => (
          <MenuItem key = { 'select-dataset-' + index } value = {dataset}> {dataset} </MenuItem>
        )) }
      </Select>
    </FormControl>
  </div> );
})

const SpecifyStrategy = observer(() => {
  const { materializationStore } = useRootStore();
  const [isStrategyDialogOpen, setStrategyDialogOpen] = React.useState(false);
  const [strategyIndex, setStrategyIndex] = React.useState(0);
  return ( <span>
    <ButtonChip label = 'Use predefined strategy' variant = 'outlined' onClick = { () => setStrategyDialogOpen(true) } />
    <Dialog open = {isStrategyDialogOpen}>
      <DialogTitle>Specify strategy</DialogTitle>
      <DialogContent>
      <FormControl sx = {{ minWidth: 200 }}>
        <InputLabel htmlFor = "select-strategy">Strategy</InputLabel>
        <Select
          id = "select-strategy" label = "Strategy"
          style = {{ maxHeight: '50px', marginBottom: '5px' }}
          value = {strategyIndex}
          onChange = { e => setStrategyIndex(e.target.value as number) }>
          { materializationStore.strategies.map((strategy, index) => (
            <MenuItem key = { 'select-strategy-' + index } value = {index}> {strategy.name} </MenuItem>
          )) }
        </Select>
        { materializationStore.strategies[strategyIndex].parameters.map(parameter => (parameter.possibleValues ?
            <Select>
              
            </Select> : <TextField>

            </TextField>
        )) }
      </FormControl>
        <DialogActions>
          <Button onClick = { () => setStrategyDialogOpen(false) }>Cancel</Button>
          <Button onClick = { () => { setStrategyDialogOpen(false); } }>Confirm</Button>
        </DialogActions>
      </DialogContent>
    </Dialog>
  </span> );
})

const ChooseCuboids = observer(() => {
  const { materializationStore } = useRootStore();
  const addCuboidsFilters = materializationStore.addCuboidsFilters;
  const dimensions = materializationStore.dimensions;
  const [isCuboidsDialogOpen, setCuboidsDialogOpen] = React.useState(false);
  return ( <span>
    <ButtonChip label = 'Choose cuboids' variant = 'outlined' onClick = { () => setCuboidsDialogOpen(true) } />
    <Dialog fullScreen open = {isCuboidsDialogOpen}>
      <DialogTitle>Choose cuboids</DialogTitle>
      <DialogContent>
        <Grid container maxHeight='30vh' overflow='scroll' style={{ paddingTop: '1px', paddingBottom: '1px' }}>
          <Grid item xs={6}>
            { materializationStore.addCuboidsFilters.map((filter, index) => {
              return (<FilterChip
                key = { 'materialization-choose-cuboids-filter-chip-' + dimensions[filter.dimensionIndex].name + '-' + filter.bitsFrom + '-' + filter.bitsTo }
                text = { dimensions[filter.dimensionIndex].name + ' / ' + filter.bitsFrom + '–' + filter.bitsTo }
                onDelete = { () => materializationStore.removeFilterAtIndex(index) }
              /> );
            }) }
            <AddCuboidsFilterChip onAdd = {(dimensionIndex: number, bitsFrom: number, bitsTo: number) => 
              materializationStore.addAddCuboidsFilter(dimensionIndex, bitsFrom, bitsTo)
            } />
          </Grid>
        </Grid>
        <Box sx = {{ height: '70vh', width: '100%', marginTop: '20px' }}>
          <DataGrid rows = {rows} columns = {columns} checkboxSelection />
        </Box>
        <DialogActions>
          <Button onClick = { () => setCuboidsDialogOpen(false) }>Cancel</Button>
          <Button onClick = { () => { setCuboidsDialogOpen(false); } }>Confirm</Button>
        </DialogActions>
      </DialogContent>
    </Dialog>
  </span> );
});