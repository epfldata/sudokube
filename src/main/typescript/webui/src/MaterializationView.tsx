import * as React from 'react';
import Container from '@mui/material/Container';
import Grid from '@mui/material/Grid';
import { FilterChip } from './QueryViewChips';
import Chip from '@mui/material/Chip';
import FilterAltIcon from '@mui/icons-material/FilterAlt';
import { DataGrid, GridActionsCellItem, GridColDef } from '@mui/x-data-grid';
import { Box, Button, Dialog, DialogActions, DialogContent, DialogTitle, FormControl, FormLabel, InputLabel, MenuItem, Select, TextField } from '@mui/material';
import { observer } from 'mobx-react-lite';
import { useRootStore } from './RootStore';
import { Cuboid, MaterializationDimension } from './MaterializationStore';
import { ButtonChip, chipStyle } from './GenericChips';
import { runInAction } from 'mobx';
import { useState } from 'react';
import DeleteIcon from '@mui/icons-material/Delete';

export default observer(function Materialization() {
  const { materializationStore: store } = useRootStore();
  const [cubeName, setCubeName] = useState('');
  // TODO: Call backend to fetch these things
  runInAction(() => {
    store.datasets = ['Sales', 'Dataset2'];
    store.selectedDataset = store.datasets[0];
    store.dimensions = [
      { name: 'Country', numBits: 6 }, 
      { name: 'City', numBits: 6 },
      { name: 'Year', numBits: 6 },
      { name: 'Month', numBits: 5 }, 
      { name: 'Day', numBits: 6 }
    ];
    store.strategies = [
      {
        name: 'Strategy', 
        parameters: [
          { name: 'Parameter1', possibleValues: ['1', '2'] },
          { name: 'Parameter2' }
        ]
      }
    ];
    // TODO: Remove this when cuboids can really be added and filtered
    store.filteredChosenCuboids = store.chosenCuboids = store.addCuboidsFilteredCuboids = [
      {
        id: "1",
        dimensions: [
          { name: "Country", bits: '\u2589\u2589\u2589\u25A2\u25A2\u25A2' },
          { name: "City", bits: '\u25A2\u25A2\u25A2\u25A2\u25A2\u25A2' },
          { name: "Year", bits: '\u25A2\u25A2\u25A2\u25A2\u25A2\u25A2' },
          { name: "Month", bits: '\u2589\u2589\u2589\u2589\u25A2' },
          { name: "Day", bits: '\u25A2\u25A2\u25A2\u25A2\u25A2\u25A2'}
        ] 
      }
    ];
  });
  return (
    <Container style={{ paddingTop: '20px' }}>
      <SelectDataset/>
      <div style = {{ marginTop: 5, marginBottom: 5 }}>
        <span><b>Add cuboids to materialize </b></span>
        <SpecifyStrategy/>
        <AddCuboids/>
      </div>
      <div>
        <div style = {{ marginTop: 5, marginBottom: 10 }}><b>Cuboids chosen</b></div>
        <ChosenCuboids />
      </div>
      <div style = {{ marginTop: 10 }}>
        <TextField
          id = 'cube-name-text-field'
          label = 'Name of cube'
          size = 'small'
          value = { cubeName }
          onChange = { e => setCubeName(e.target.value) }
        />
        <Button onClick = { () => {
          // TODO: Call backend with list of cuboids to materialize
        } }>Materialize</Button>
      </div>
    </Container>
  )
})

const SelectDataset = observer(() => {
  const { materializationStore: store } = useRootStore();
  return ( <div>
    <FormControl sx = {{ minWidth: 200 }}>
      <InputLabel htmlFor = "select-dataset">Dataset</InputLabel>
      <Select
        id = "select-dataset" label = "Dataset"
        style = {{ maxHeight: '50px', marginBottom: '5px' }}
        value = { store.selectedDataset }
        onChange = { e => runInAction(() => {
            store.selectedDataset = e.target.value;
            // TODO: Call backend to update dimensions
            store.dimensions = store.dimensions;
            store.chosenCuboids = [];
            store.chosenCuboidsFilters = [];
            store.filteredChosenCuboids = [];
        })}>
        { store.datasets.map((dataset, index) => (
          <MenuItem key = { 'select-dataset-' + index } value = {dataset}>{dataset}</MenuItem>
        )) }
      </Select>
    </FormControl>
  </div> );
})

const SpecifyStrategy = observer(() => {
  const { materializationStore: store } = useRootStore();
  const [isDialogOpen, setDialogOpen] = React.useState(false);
  return ( <span>
    <ButtonChip label = 'Use predefined strategy' variant = 'outlined' onClick = { () => setDialogOpen(true) } />
    <Dialog open = {isDialogOpen}>
      <DialogTitle>Specify strategy</DialogTitle>
      <DialogContent>
        <FormControl sx = {{ minWidth: 200, marginTop: 1 }}>
          <InputLabel htmlFor = 'select-strategy'>Strategy</InputLabel>
          <Select
            id = 'select-strategy' label = 'Strategy'
            style = {{ maxHeight: '50px', marginBottom: '5px' }}
            value = { store.strategyIndex | 0 }
            onChange = { e => runInAction(() => {
              const strategyIndex = e.target.value as number;
              store.strategyIndex = strategyIndex;
              store.strategyParameters = store.strategies[strategyIndex].parameters.map(p => p.possibleValues ? p.possibleValues[0] : '');
            })}>
            { store.strategies.map((strategy, index) => (
              <MenuItem key = { 'select-strategy-' + index } value = {index}> {strategy.name} </MenuItem>
            )) }
          </Select>
          { store.strategies[store.strategyIndex].parameters.map((parameter, index) => (
              parameter.possibleValues ?
                <FormControl 
                  key = { 'strategy-' + store.strategyIndex + '-param-' + index }
                  sx = {{ minWidth: 200, marginTop: 1 }}
                >
                  <InputLabel htmlFor = { 'strategy-' + store.strategyIndex + '-param-' + index }>{parameter.name}</InputLabel>
                  <Select
                    id = { 'strategy-' + store.strategyIndex + '-param-' + index }
                    label = { parameter.name }
                    style = {{ marginBottom: 5 }}
                    value = { store.strategyParameters[index] }
                    onChange = { e => runInAction(() => store.strategyParameters[index] = e.target.value) }>
                    { store.strategies[store.strategyIndex].parameters[index].possibleValues!.map((value, valueIndex) => (
                      <MenuItem
                        key = { 'strategy-' + store.strategyIndex + '-param-' + index + '-select-item-' + valueIndex }
                        value = { value }>
                        {value}
                      </MenuItem>
                    )) }
                  </Select> 
                </FormControl>
              : <TextField
                  id = { 'strategy-' + store.strategyIndex + '-param-' + index }
                  key = { 'strategy-' + store.strategyIndex + '-param-' + index }
                  label = { parameter.name }
                  style = {{ marginBottom: 5 }}
                  value = { store.strategyParameters[index] }
                  onChange = { e => runInAction(() => store.strategyParameters[index] = e.target.value) }
                />
          ))}
        </FormControl>
        <DialogActions>
          <Button onClick = { () => setDialogOpen(false) }>Cancel</Button>
          <Button onClick = { () => { 
            // TODO: Send strategy and parameters to backend
            setDialogOpen(false); 
          } }>Confirm</Button>
        </DialogActions>
      </DialogContent>
    </Dialog>
  </span> );
})

const AddCuboids = observer(() => {
  const { materializationStore: store } = useRootStore();
  const dimensions = store.dimensions;
  const [isCuboidsDialogOpen, setCuboidsDialogOpen] = React.useState(false);
  // TODO: Call backend to get all cuboids
  return ( <span>
    <ButtonChip label = 'Manually choose cuboids' variant = 'outlined' onClick = { () => setCuboidsDialogOpen(true) } />
    <Dialog fullScreen open = {isCuboidsDialogOpen}>
      <DialogTitle>Choose cuboids</DialogTitle>
      <DialogContent>
        <Grid container maxHeight='30vh' overflow='scroll' style={{ paddingTop: '1px', paddingBottom: '1px' }}>
          <Grid item xs={6}>
            { store.addCuboidsFilters.map((filter, index) => {
              return (<FilterChip
                key = { 'materialization-choose-cuboids-filter-chip-' + dimensions[filter.dimensionIndex].name + '-' + filter.bitsFrom + '-' + filter.bitsTo }
                text = { dimensions[filter.dimensionIndex].name + ' / ' + filter.bitsFrom + '–' + filter.bitsTo }
                onDelete = { () => runInAction(() => store.addCuboidsFilters.splice(index, 1)) }
              /> );
            }) }
            <AddCuboidsFilterChip onAdd = {(dimensionIndex: number, bitsFrom: number, bitsTo: number) => {
              runInAction(() => store.addCuboidsFilters.push({
                dimensionIndex: dimensionIndex,
                bitsFrom: bitsFrom,
                bitsTo: bitsTo
              }));
              // TODO: Call backend with filters to get matching cuboids
            }} />
          </Grid>
        </Grid>

        <Box sx = {{ height: '70vh', width: '100%', marginTop: '20px' }}>
          <DataGrid
            rows = { store.addCuboidsFilteredCuboids.map(cuboidToRow) } 
            columns = { store.dimensions.map(dimensionToColumn) }
            checkboxSelection sx = {{ overflowX: 'scroll' }} 
          />
        </Box>

        <DialogActions>
          <Button onClick = { () => setCuboidsDialogOpen(false) }>Cancel</Button>
          <Button onClick = { () => { 
            // TODO: Probably call backend to refresh the list of cuboids in the outer materialization view
            setCuboidsDialogOpen(false); 
          } }>Confirm</Button>
        </DialogActions>
      </DialogContent>
    </Dialog>
  </span> );
});

const ChosenCuboids = observer(() => {
  const { materializationStore: store } = useRootStore();
  const dimensions = store.dimensions;
  return ( <div>
    <Grid container maxHeight='30vh' overflow='scroll' style={{ paddingTop: '1px', paddingBottom: '1px' }}>
      <Grid item xs={6}>
        { store.chosenCuboidsFilters.map((filter, index) => (
          <FilterChip
            key = { 'materialization-filter-chip-' + dimensions[filter.dimensionIndex].name + '-' + filter.bitsFrom + '-' + filter.bitsTo }
            text = { dimensions[filter.dimensionIndex].name + ' / ' + filter.bitsFrom + '–' + filter.bitsTo }
            onDelete = { () => runInAction(() => store.addCuboidsFilters.splice(index, 1)) }
          /> 
        )) }
        <AddCuboidsFilterChip onAdd = {(dimensionIndex: number, bitsFrom: number, bitsTo: number) => 
          runInAction(() => store.chosenCuboidsFilters.push({
            dimensionIndex: dimensionIndex,
            bitsFrom: bitsFrom,
            bitsTo: bitsTo
          }))
        } />
      </Grid>
    </Grid>
    <Box sx = {{ height: '65vh', width: '100%', marginTop: '20px' }}>
      <DataGrid
        rows = { store.chosenCuboids.map(cuboidToRow) }
        columns = {
          store.dimensions.map(dimensionToColumn)
            .concat([{
              field: 'actions',
              type: 'actions',
              getActions: (params) => [
                <GridActionsCellItem
                  icon = {<DeleteIcon/>}
                  label = 'Delete'
                  onClick={() => {
                    // Todo: delete
                  }}
                />
              ]
            } as GridColDef])
        }
        disableRowSelectionOnClick
        sx = {{ overflowX: 'scroll' }}
      />
    </Box>
  </div>
  )
})

function AddCuboidsFilterChip({onAdd}: {
  onAdd: (dimensionIndex: number, bitsFrom: number, bitsTo: number) => void
}) {
  const { materializationStore: store } = useRootStore();
  const [isDialogOpen, setDialogOpen] = React.useState(false);
  const [dimensionIndex, setDimensionIndex] = React.useState(0);
  const [bitsFrom, setBitsFrom] = React.useState('');
  const [bitsTo, setBitsTo] = React.useState('');
  return (<span>
    <Chip
      icon = { <FilterAltIcon style = {{ height: '18px' }} /> }
      label = 'Add filter'
      onClick = { () => setDialogOpen(true) }
      style = {chipStyle}
      variant = 'outlined'
      color = 'primary'
    />
    <Dialog open = {isDialogOpen}>
      <DialogTitle>Add Filter</DialogTitle>
      <DialogContent>
        <FormControl sx={{ m: 1, minWidth: 120 }}>
          <InputLabel htmlFor="add-cuboids-filter-select-dimension">Dimension</InputLabel>
          <Select
            value = {dimensionIndex}
            onChange = { e => setDimensionIndex(e.target.value as number) }
            id="add-cuboids-filter-select-dimension" label="Dimension">
            { store.dimensions.map((dimension, index) => (
              <MenuItem key = { 'add-cuboids-filter-select-dimension-' + index } value = {index}> {dimension.name} </MenuItem>
            )) }
          </Select>
        </FormControl>
        <FormControl sx={{ m: 1, minWidth: 120 }}>
          <FormLabel id="add-cuboids-filter-column-range-label" style = {{ fontSize: 'small' }}>Columns</FormLabel>
          <span aria-labelledby = "add-cuboids-filter-column-range-label">
            <TextField size = 'small' sx = {{ width: 60 }} onChange = { e => setBitsFrom(e.target.value) } />
            <span style = {{ lineHeight: 2.5 }}> – </span>
            <TextField size = 'small' sx = {{ width: 60 }} onChange = { e => setBitsTo(e.target.value) } />
          </span>
        </FormControl>
        <DialogActions>
          <Button onClick = { () => setDialogOpen(false) }>Cancel</Button>
          <Button onClick = { () => {
            onAdd(dimensionIndex, parseInt(bitsFrom), parseInt(bitsTo));
            setDialogOpen(false);
          } }>Add</Button>
        </DialogActions>
      </DialogContent>
    </Dialog>
  </span>)
}

const cuboidToRow = ((cuboid: Cuboid) => {
  let row: any = {};
  row["id"] = cuboid.id;
  cuboid.dimensions.forEach(dimension => row[dimension.name] = dimension.bits);
  return row;
});

const dimensionToColumn = ((dimension: MaterializationDimension) => ({
  field: dimension.name,
  type: 'string',
  headerName: dimension.name + ' (' + dimension.numBits + ' bits)',
  flex: 1,
  sortable: false,
  disableColumnMenu: true
} as GridColDef));
