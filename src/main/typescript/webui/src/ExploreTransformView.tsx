import { Button, Container, FormControl, InputLabel, MenuItem, Select, Table, TableBody, TableCell, TableContainer, TableHead, TableRow } from "@mui/material";
import { observer } from "mobx-react-lite";
import React, { ReactNode, useState } from "react";
import { useRootStore } from "./RootStore";
import { ExploreDimension } from "./ExploreTransformStore";
import CheckCircleOutlineIcon from '@mui/icons-material/CheckCircleOutline';
import CancelIcon from '@mui/icons-material/Cancel';
import { runInAction } from "mobx";

export default observer(function ExploreTransformView() {
  const { exploreTransformStore: store } = useRootStore();
  runInAction(() => {
    // TODO: Call backend to fetch cubes
    store.cubes = ['sales'];
    store.selectedCubeIndex = 0;
    // TODO: Call backend to fetch these things for the default cube
    store.dimensions = [
      new ExploreDimension('Country', 6), 
      new ExploreDimension('City', 6), 
      new ExploreDimension('Year', 6), 
      new ExploreDimension('Month', 5), 
      new ExploreDimension('Day', 6)
    ];
    store.timeNumBits = 16;
    store.timeBitsFilters.push(0);
    store.timeBitsFilters.push(1);
  })
  return (
    <Container style = {{ paddingTop: '20px' }}>
      <SelectCube/>
      <CheckRename/>
      <FindRenameTime/>
      <Merge/>
    </Container>
  );
})

const SelectCube = observer(() => {
  const { exploreTransformStore: store } = useRootStore();
  return ( <div>
    <FormControl sx = {{ minWidth: 200 }}>
      <InputLabel htmlFor = "select-cube">Select Cube</InputLabel>
      <Select
        id = "select-cube" label = "Select Cube"
        style = {{ marginBottom: 10 }}
        size = 'small'
        value = { store.selectedCubeIndex }
        onChange = { e => {
          runInAction(() => store.selectedCubeIndex = e.target.value as number);
          // TODO: Load dimensions and number of time bits
        } }>
        { store.cubes.map((cube, index) => (
          <MenuItem key = { 'select-cube-' + index } value = {index}>{cube}</MenuItem>
        )) }
      </Select>
    </FormControl>
  </div> );
})

const CheckRename = observer(() => {
  const { exploreTransformStore: store } = useRootStore();
  const [isRenameCheckResultShown, setRenameCheckResultShown] = useState(false);
  return ( <div>
    <h3>Check potentially renamed dimensions pair</h3>
    <div>
      <SelectDimension 
        id = 'check-rename-select-dimension-1'
        label = 'Dimension 1'
        value = { store.checkRenameDimension1Index }
        onChange = { v => {
          runInAction(() => store.checkRenameDimension1Index = v as number);
          setRenameCheckResultShown(false);
        }}
        dimensionToMenuItemMapper = { (dimension, index) => (
          <MenuItem key = { 'check-rename-select-dimension-1-' + index } value = {index}>{dimension.name}</MenuItem>
        ) }
      />
      <SelectDimension 
        id = 'check-rename-select-dimension-2'
        label = 'Dimension 2'
        value = { store.checkRenameDimension2Index }
        onChange = { v => {
          runInAction(() => store.checkRenameDimension2Index = v);
          setRenameCheckResultShown(false);
        }}
        dimensionToMenuItemMapper = { (dimension, index) => (
          <MenuItem key = { 'check-rename-select-dimension-2-' + index } value = {index}>{dimension.name}</MenuItem>
        ) }
      />
      <Button onClick = {() => {
        // TODO: Query backend and show results
        runInAction(() => store.checkRenameResult = true);
        setRenameCheckResultShown(true);
      }}>Check</Button>
    </div>
    <div hidden = {!isRenameCheckResultShown}> { (() => {
      let dimension1 = store.dimensions[store.checkRenameDimension1Index].name;
      let dimension2 = store.dimensions[store.checkRenameDimension2Index].name;
      return store.checkRenameResult
        ? <span style = {{display: 'flex'}}><CheckCircleOutlineIcon/> {dimension1} is likely renamed to {dimension2}.</span>
        : <span style = {{display: 'flex'}}><CancelIcon/>{dimension1} is not renamed to {dimension2}.</span>
    })()} </div>
  </div> );
})

const FindRenameTime = observer(() => {
  const { exploreTransformStore: store } = useRootStore();
  const [isStepShown, setStepShown] = useState(false);
  const [isSearchEnded, setSearchEnded] = useState(false);
  return ( <div>
    <h3>Find time of rename</h3>
    <div>
      <SelectDimension
        id = 'rename-time-select-dimension'
        label = 'Dimension'
        value = { store.findRenameTimeDimensionIndex }
        onChange = { v => {
          runInAction(() => store.findRenameTimeDimensionIndex = v);
          setStepShown(false);
          setSearchEnded(false);
          runInAction(() => store.timeBitsFilters = []);
        }}
        dimensionToMenuItemMapper = { (dimension, index) => (
          <MenuItem key = { 'check-rename-select-dimension-2-' + index } value = {index}>{dimension.name}</MenuItem>
        ) }
      />
      <Button onClick = {() => {
        // TODO: Query the first bit
        setStepShown(true);
      }}>Start</Button>
    </div>
    <div hidden = {!isStepShown}>
      <div>
        Filters on time bits applied: 
        { store.timeBitsFilters.map(bit => bit == 0 ? " \u25A2 " : " \u2589 ") }
        { Array(store.timeNumBits - store.timeBitsFilters.length).fill(" \u2715 ") }
      </div>
      <TableContainer>
        <Table sx = {{ minWidth: 400 }} aria-label = "simple table">
          <TableHead>
            <TableRow>
              <TableCell>Time range</TableCell>
              <TableCell>Count</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            <TableRow>
              <TableCell>{store.timeRange1}</TableCell>
              <TableCell>{store.count1}</TableCell>
            </TableRow>
            <TableRow>
              <TableCell>{store.timeRange2}</TableCell>
              <TableCell>{store.count2}</TableCell>
            </TableRow>
          </TableBody>
        </Table>
      </TableContainer>
    </div>
    <Button 
      disabled = {isSearchEnded} 
      onClick = {() => {
        // TODO: Implement logic to continue or complete the search, get and show result
        runInAction(() => store.timeBitsFilters.push(0));
        setSearchEnded(true);
        runInAction(() => { store.renameTime = "..." });
      }}>
      Continue
    </Button>
    <div hidden = {!isSearchEnded}>Dimension is renamed at {store.renameTime}</div>
  </div> );
})

const Merge = observer(() => {
  const { exploreTransformStore: store } = useRootStore();
  const [isComplete, setComplete] = useState(false);
  return ( <div>
    <h3>Merge dimensions</h3>
    <SelectDimension 
      id = 'merge-select-dimension-1'
      label = 'Dimension 1'
      value = { store.mergeDimension1Index }
      onChange = { v => runInAction(() => store.mergeDimension1Index = v) }
      dimensionToMenuItemMapper = { (dimension, index) => (
        <MenuItem key = { 'merge-select-dimension-1-' + index } value = {index}>{dimension.name}</MenuItem>
      ) }
    />
    <SelectDimension 
      id = 'merge-select-dimension-2'
      label = 'Dimension 2'
      value = { store.mergeDimension2Index }
      onChange = { v => runInAction(() => store.mergeDimension2Index = v) }
      dimensionToMenuItemMapper = { (dimension, index) => (
        <MenuItem key = { 'merge-select-dimension-2-' + index } value = {index}>{dimension.name}</MenuItem>
      ) }
    />
    <Button onClick = {() => {
      // TODO: Query backend, show new name
      setComplete(true);
      runInAction(() => store.newCubeName = 'sales-merged')
    }}>Merge and generate new cube</Button>
    <div style = {{ display: isComplete ? 'flex' : 'none' }}>
      <CheckCircleOutlineIcon/>Cube saved as {store.newCubeName}
    </div>
  </div> );
})

const SelectDimension = observer(({ id, label, value, onChange, dimensionToMenuItemMapper }: {
  id: string,
  label: string,
  value: any,
  onChange: (value: any) => void,
  dimensionToMenuItemMapper: (dimension: ExploreDimension, index: number) => ReactNode
}) => {
  const { exploreTransformStore } = useRootStore();
  return (
    <FormControl sx = {{ minWidth: 200 }}>
      <InputLabel htmlFor = {id}>{label}</InputLabel>
      <Select
        id = {id} label = {label}
        style = {{ marginBottom: 10, marginRight: 10 }}
        size = 'small'
        value = { value }
        onChange = { e => onChange(e.target.value as number) }>
        { exploreTransformStore.dimensions.map(dimensionToMenuItemMapper) }
      </Select>
    </FormControl>
  );
});