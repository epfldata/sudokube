import { Button, Container, FormControl, InputLabel, MenuItem, Select, Table, TableBody, TableCell, TableContainer, TableHead, TableRow } from "@mui/material";
import { observer } from "mobx-react-lite";
import React, { ReactNode, useState } from "react";
import { useRootStore } from "./RootStore";
import { ExploreDimension } from "./ExploreTransformStore";
import CheckCircleOutlineIcon from '@mui/icons-material/CheckCircleOutline';
import CancelIcon from '@mui/icons-material/Cancel';

export default observer(function ExploreTransformView() {
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
  const { exploreTransformStore } = useRootStore();
  return ( <div>
    <FormControl sx = {{ minWidth: 200 }}>
      <InputLabel htmlFor = "select-cube">Select Cube</InputLabel>
      <Select
        id = "select-cube" label = "Select Cube"
        style = {{ marginBottom: 10 }}
        size = 'small'
        value = { exploreTransformStore.selectedCubeIndex }
        onChange = { e => {
          exploreTransformStore.setCubeIndex(e.target.value as number);
          // TODO: Load dimensions
        } }>
        { exploreTransformStore.cubes.map((cube, index) => (
          <MenuItem key = { 'select-cube-' + index } value = {index}>{cube}</MenuItem>
        )) }
      </Select>
    </FormControl>
  </div> );
})

const CheckRename = observer(() => {
  const { exploreTransformStore } = useRootStore();
  const [isRenameCheckResultShown, setRenameCheckResultShown] = useState(false);
  return ( <div>
    <h3>Check potentially renamed dimensions pair</h3>
    <div>
      <SelectDimension 
        id = 'check-rename-select-dimension-1'
        label = 'Dimension 1'
        value = { exploreTransformStore.checkRenameDimension1Index }
        onChange = { v => {
          exploreTransformStore.setCheckRenameDimension1Index(v);
          setRenameCheckResultShown(false);
        }}
        dimensionToMenuItem = { (dimension, index) => (
          <MenuItem key = { 'check-rename-select-dimension-1-' + index } value = {index}>{dimension.name}</MenuItem>
        ) }
      />
      <SelectDimension 
        id = 'check-rename-select-dimension-2'
        label = 'Dimension 2'
        value = { exploreTransformStore.checkRenameDimension2Index }
        onChange = { v => {
          exploreTransformStore.setCheckRenameDimension2Index(v);
          setRenameCheckResultShown(false);
        }}
        dimensionToMenuItem = { (dimension, index) => (
          <MenuItem key = { 'check-rename-select-dimension-2-' + index } value = {index}>{dimension.name}</MenuItem>
        ) }
      />
      <Button onClick = {() => {
        // TODO: Query backend and show results
        exploreTransformStore.setCheckRenameResult(true);
        setRenameCheckResultShown(true);
      }}>Check</Button>
    </div>
    <div hidden = {!isRenameCheckResultShown}> { (() => {
      let dimension1 = exploreTransformStore.dimensions[exploreTransformStore.checkRenameDimension1Index].name;
      let dimension2 = exploreTransformStore.dimensions[exploreTransformStore.checkRenameDimension2Index].name;
      return exploreTransformStore.checkRenameResult
        ? <span style = {{display: 'flex'}}><CheckCircleOutlineIcon/> {dimension1} is likely renamed to {dimension2}.</span>
        : <span style = {{display: 'flex'}}><CancelIcon/>{dimension1} is not renamed to {dimension2}.</span>
    })()} </div>
  </div> );
})

const FindRenameTime = observer(() => {
  const { exploreTransformStore } = useRootStore();
  const [isStepShown, setStepShown] = useState(false);
  const [isSearchEnded, setSearchEnded] = useState(false);
  return ( <div>
    <h3>Find time of rename</h3>
    <div>
      <SelectDimension
        id = 'rename-time-select-dimension'
        label = 'Dimension'
        value = { exploreTransformStore.findRenameTimeDimensionIndex }
        onChange = { v => {
          exploreTransformStore.setFindRenameTimeDimensionIndex(v);
          setStepShown(false);
          setSearchEnded(false);
          exploreTransformStore.clearTimeBitsFilters();
        }}
        dimensionToMenuItem = { (dimension, index) => (
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
        { exploreTransformStore.timeBitsFilters.map(bit => bit == 0 ? " \u25A2 " : " \u2589 ") }
        { Array(exploreTransformStore.timeNumBits - exploreTransformStore.timeBitsFilters.length).fill(" \u2715 ") }
      </div>
      <TableContainer>
        <Table sx={{ minWidth: 400 }} aria-label="simple table">
          <TableHead>
            <TableRow>
              <TableCell>Time range</TableCell>
              <TableCell>Count</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            <TableRow>
              <TableCell>{exploreTransformStore.timeRange1}</TableCell>
              <TableCell>{exploreTransformStore.count1}</TableCell>
            </TableRow>
            <TableRow>
              <TableCell>{exploreTransformStore.timeRange2}</TableCell>
              <TableCell>{exploreTransformStore.count2}</TableCell>
            </TableRow>
          </TableBody>
        </Table>
      </TableContainer>
    </div>
    <Button 
      disabled = {isSearchEnded} 
      onClick = {() => {
        // TODO: Implement logic to continue the search
        setSearchEnded(true);
      }}>
      Continue
    </Button>
    <div hidden = {!isSearchEnded}>Dimension is renamed at {exploreTransformStore.renameTime}</div>
  </div> );
})

const Merge = observer(() => {
  const { exploreTransformStore } = useRootStore();
  const [isComplete, setComplete] = useState(false);
  return ( <div>
    <h3>Merge dimensions</h3>
    <SelectDimension 
      id = 'merge-select-dimension-1'
      label = 'Dimension 1'
      value = { exploreTransformStore.mergeDimension1Index }
      onChange = { v => exploreTransformStore.setMergeDimension1Index(v) }
      dimensionToMenuItem = { (dimension, index) => (
        <MenuItem key = { 'merge-select-dimension-1-' + index } value = {index}>{dimension.name}</MenuItem>
      ) }
    />
    <SelectDimension 
      id = 'merge-select-dimension-2'
      label = 'Dimension 2'
      value = { exploreTransformStore.mergeDimension2Index }
      onChange = { v => exploreTransformStore.setMergeDimension2Index(v) }
      dimensionToMenuItem = { (dimension, index) => (
        <MenuItem key = { 'merge-select-dimension-2-' + index } value = {index}>{dimension.name}</MenuItem>
      ) }
    />
    <Button onClick = {() => {
      // TODO: Query backend and show results somehow
      setComplete(true);
    }}>Merge and generate new cube</Button>
    <div style = {{ display: isComplete ? 'flex' : 'none' }}>
      <CheckCircleOutlineIcon/>Cube saved as {exploreTransformStore.newCubeName}
    </div>
  </div> );
})

const SelectDimension = observer(({ id, label, value, onChange, dimensionToMenuItem }: {
  id: string,
  label: string,
  value: any,
  onChange: (value: any) => void,
  dimensionToMenuItem: (dimension: ExploreDimension, index: number) => ReactNode
}) => {
  const { exploreTransformStore } = useRootStore();
  return (<FormControl sx = {{ minWidth: 200 }}>
    <InputLabel htmlFor = {id}>{label}</InputLabel>
    <Select
      id = {id} label = {label}
      style = {{ marginBottom: 10, marginRight: 10 }}
      size = 'small'
      value = { value }
      onChange = { e => onChange(e.target.value as number) }>
      { exploreTransformStore.dimensions.map(dimensionToMenuItem) }
    </Select>
  </FormControl>);
});