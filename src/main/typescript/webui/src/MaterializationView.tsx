import * as React from 'react';
import Container from '@mui/material/Container';
import Grid from '@mui/material/Grid';
import { FilterChip } from './QueryViewChips';
import Chip from '@mui/material/Chip';
import FilterAltIcon from '@mui/icons-material/FilterAlt';
import { DataGrid } from '@mui/x-data-grid';
import { Box, Button, Dialog, DialogActions, DialogContent, DialogTitle, FormControl, FormLabel, InputLabel, MenuItem, Select, TextField } from '@mui/material';
import { observer } from 'mobx-react-lite';
import { useRootStore } from './RootStore';
import { Cuboid, MaterializationDimension } from './MaterializationStore';
import { ButtonChip, chipStyle } from './GenericChips';

export default observer(function Materialization() {
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
      <Button style = {{ marginTop: 10 }} onClick = { () => {
        // TODO: Call backend with list of cuboids to materialize
      } }>Materialize</Button>
    </Container>
  )
})

const SelectDataset = observer(() => {
  const { materializationStore } = useRootStore();
  return ( <div>
    <FormControl sx = {{ minWidth: 200 }}>
      <InputLabel htmlFor = "select-dataset">Dataset</InputLabel>
      <Select
        id = "select-dataset" label = "Dataset"
        style = {{ maxHeight: '50px', marginBottom: '5px' }}
        value = { materializationStore.selectedDataset }
        onChange = { e => materializationStore.setDatasetAndLoadDimensions(e.target.value) }>
        { materializationStore.datasets.map((dataset, index) => (
          <MenuItem key = { 'select-dataset-' + index } value = {dataset}>{dataset}</MenuItem>
        )) }
      </Select>
    </FormControl>
  </div> );
})

const SpecifyStrategy = observer(() => {
  const { materializationStore } = useRootStore();
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
            value = {materializationStore.strategyIndex}
            onChange = { e => materializationStore.setStrategyIndex(e.target.value as number) }>
            { materializationStore.strategies.map((strategy, index) => (
              <MenuItem key = { 'select-strategy-' + index } value = {index}> {strategy.name} </MenuItem>
            )) }
          </Select>
          { materializationStore.strategies[materializationStore.strategyIndex].parameters.map((parameter, index) => (
              parameter.possibleValues ?
                <FormControl sx = {{ minWidth: 200, marginTop: 1 }}>
                  <InputLabel htmlFor = { 'strategy-' + materializationStore.strategyIndex + '-param-' + index }>{parameter.name}</InputLabel>
                  <Select
                    id = { 'strategy-' + materializationStore.strategyIndex + '-param-' + index }
                    key = { 'strategy-' + materializationStore.strategyIndex + '-param-' + index }
                    label = { parameter.name }
                    style = {{ marginBottom: 5 }}
                    value = { materializationStore.strategyParameters[index] }
                    onChange = { e => materializationStore.setStrategyParameter(index, e.target.value) }>
                    { materializationStore.strategies[materializationStore.strategyIndex].parameters[index].possibleValues!.map((value, valueIndex) => (
                      <MenuItem
                        key = { 'strategy-' + materializationStore.strategyIndex + '-param-' + index + '-select-item-' + valueIndex }
                        value = {index}>
                        {value}
                      </MenuItem>
                    )) }
                  </Select> 
                </FormControl>
              : <TextField
                  id = { 'strategy-' + materializationStore.strategyIndex + '-param-' + index }
                  key = { 'strategy-' + materializationStore.strategyIndex + '-param-' + index }
                  label = { parameter.name }
                  style = {{ marginBottom: 5 }}
                  value = { materializationStore.strategyParameters[index] }
                  onChange = { e => materializationStore.setStrategyParameter(index, e.target.value) }
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
  const { materializationStore } = useRootStore();
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
                onDelete = { () => materializationStore.removeAddCuboidsFilterAtIndex(index) }
              /> );
            }) }
            <AddCuboidsFilterChip onAdd = {(dimensionIndex: number, bitsFrom: number, bitsTo: number) => {
              materializationStore.addAddCuboidsFilter(dimensionIndex, bitsFrom, bitsTo);
              // TODO: Call backend with filters to get matching cuboids
            }} />
          </Grid>
        </Grid>

        <Box sx = {{ height: '70vh', width: '100%', marginTop: '20px' }}>
          <DataGrid
            rows = { materializationStore.addCuboidsFilteredCuboids.map(cuboidToRow) } 
            columns = { materializationStore.dimensions.map(dimensionToColumn) }
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
  const { materializationStore } = useRootStore();
  const dimensions = materializationStore.dimensions;
  return ( <div>
    <Grid container maxHeight='30vh' overflow='scroll' style={{ paddingTop: '1px', paddingBottom: '1px' }}>
    <Grid item xs={6}>
      { materializationStore.chosenCuboidsFilters.map((filter, index) => (
        <FilterChip
          key = { 'materialization-filter-chip-' + dimensions[filter.dimensionIndex].name + '-' + filter.bitsFrom + '-' + filter.bitsTo }
          text = { dimensions[filter.dimensionIndex].name + ' / ' + filter.bitsFrom + '–' + filter.bitsTo }
          onDelete = { () => materializationStore.removeAddCuboidsFilterAtIndex(index) }
        /> 
      )) }
      <AddCuboidsFilterChip onAdd = {(dimensionIndex: number, bitsFrom: number, bitsTo: number) => 
        materializationStore.addChosenCuboidsFilter(dimensionIndex, bitsFrom, bitsTo) 
      } />
    </Grid>
  </Grid>
  <Box sx = {{ height: '65vh', width: '100%', marginTop: '20px' }}>
    <DataGrid
      rows = { materializationStore.chosenCuboids.map(cuboidToRow) }
      columns = { materializationStore.dimensions.map(dimensionToColumn) }
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
  const { materializationStore } = useRootStore();
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
            { materializationStore.dimensions.map((dimension, index) => (
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
  headerName: dimension.name + ' (' + dimension.numBits + ' bits)',
  flex: 1,
  sortable: false,
  disableColumnMenu: true
}));
