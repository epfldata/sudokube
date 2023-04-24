import { Button, Container, FormControl, InputLabel, MenuItem, Select, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, TextField } from "@mui/material";
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
    store.timeBitsFilters = [0, 1];
  })
  return (
    <Container style = {{ paddingTop: '20px' }}>
      <SelectCube/>
      <CheckRename/>
      <FindRenameTime/>
      <Transform/>
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
      <SingleSelectWithLabel 
        id = 'check-rename-select-dimension-1'
        label = 'Dimension 1'
        value = { store.checkRenameDimension1Index }
        options = { store.dimensions }
        onChange = { v => {
          runInAction(() => store.checkRenameDimension1Index = v as number);
          setRenameCheckResultShown(false);
        }}
        optionToMenuItemMapper = { (dimension, index) => (
          <MenuItem key = { 'check-rename-select-dimension-1-' + index } value = {index}>{dimension.name}</MenuItem>
        ) }
      />
      <SingleSelectWithLabel 
        id = 'check-rename-select-dimension-2'
        label = 'Dimension 2'
        value = { store.checkRenameDimension2Index }
        options = { store.dimensions }
        onChange = { v => {
          runInAction(() => store.checkRenameDimension2Index = v);
          setRenameCheckResultShown(false);
        }}
        optionToMenuItemMapper = { (dimension, index) => (
          <MenuItem key = { 'check-rename-select-dimension-2-' + index } value = {index}>{dimension.name}</MenuItem>
        ) }
      />
      <Button onClick = {() => {
        // TODO: Query backend and show results
        runInAction(() => store.checkRenameResult = true);
        setRenameCheckResultShown(true);
      }}>Check</Button>
    </div>
    <div hidden = {!isRenameCheckResultShown}>
      <TableContainer>
        <Table sx = {{ width: 'auto' }} aria-label = "simple table">
          <TableHead>
            <TableRow>
              <CompactTableCell>Dimension</CompactTableCell>
              <CompactTableCell>NULL</CompactTableCell>
              <CompactTableCell>Not NULL</CompactTableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            { store.dimensionsNullCount.map(row => (
              <TableRow>
                <CompactTableCell>{row.dimension}</CompactTableCell>
                <CompactTableCell>{row.nullCount}</CompactTableCell>
                <CompactTableCell>{row.notNullCount}</CompactTableCell>
              </TableRow>
            )) }
          </TableBody>
        </Table>
      </TableContainer>
      { store.checkRenameResult
          ? <span style = {{display: 'flex'}}>
              <CheckCircleOutlineIcon/>
              {store.dimensionsNullCount[0].dimension} is likely renamed to {store.dimensionsNullCount[1].dimension}.
            </span>
          : <span style = {{display: 'flex'}}>
              <CancelIcon/>
              {store.dimensionsNullCount[0].dimension} is not renamed to {store.dimensionsNullCount[1].dimension}.
            </span>
      }
    </div>
  </div> );
})

const FindRenameTime = observer(() => {
  const { exploreTransformStore: store } = useRootStore();
  const [isStepShown, setStepShown] = useState(false);
  const [isSearchEnded, setSearchEnded] = useState(false);

  const filterSquares = () => {
    let chars: string[] = store.timeBitsFilters.map(bit => bit.toString());
    if (store.timeBitsFilters.length < store.timeNumBits) {
      chars.push('?');
    }
    const remainingBits = store.timeNumBits - store.timeBitsFilters.length - 1;
    if (remainingBits > 0) {
      chars = chars.concat(Array(remainingBits).fill('✕'));
    }
    return (<span>
      { chars.map(char => <CharInSquare char = { char } emphasize = { char === '?' } />) }
    </span>)
  }

  return ( <div>
    <h3>Find time of rename</h3>

    <div>
      <SingleSelectWithLabel
        id = 'rename-time-select-dimension'
        label = 'Dimension'
        value = { store.findRenameTimeDimensionIndex }
        options = { store.dimensions }
        onChange = { v => {
          runInAction(() => store.findRenameTimeDimensionIndex = v);
          setStepShown(false);
          setSearchEnded(false);
          runInAction(() => store.timeBitsFilters = []);
        }}
        optionToMenuItemMapper = { (dimension, index) => (
          <MenuItem key = { 'check-rename-select-dimension-2-' + index } value = {index}>{dimension.name}</MenuItem>
        ) }
      />
      <Button onClick = {() => {
        // TODO: Query the first bit
        setStepShown(true);
      }}>Start</Button>
    </div>

    <div hidden = {!isStepShown}>
      <div>Filters on time bits applied: { filterSquares() }</div>
      <TableContainer>
        <Table sx = {{ width: 'auto' }} aria-label = "simple table">
          <TableHead>
            <TableRow>
              <CompactTableCell>Time range</CompactTableCell>
              <CompactTableCell>NULL</CompactTableCell>
              <CompactTableCell>Not NULL</CompactTableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            { store.timeRangeCounts.map(row => (
              <TableRow>
                <CompactTableCell>{row.range}</CompactTableCell>
                <CompactTableCell>{row.nullCount}</CompactTableCell>
                <CompactTableCell>{row.notNullCount}</CompactTableCell>
              </TableRow>
            )) }
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

const Transform = observer(() => {
  const { exploreTransformStore: store } = useRootStore();
  const [isComplete, setComplete] = useState(false);
  return ( <div>
    <h3>Transform</h3>
    <div>
      <SingleSelectWithLabel
        id = 'transform-select'
        label = 'Transformation'
        value = {0}
        options = { ['Merge'] }
        onChange = { () => {} }
        optionToMenuItemMapper = { (transformation, index) => (
          <MenuItem key = { 'transform-select-' + index } value = {index}>{transformation}</MenuItem>
        ) }
      />
    </div>
    <div>
      <SingleSelectWithLabel 
        id = 'merge-select-dimension-1'
        label = 'Dimension 1'
        value = { store.mergeDimension1Index }
        options = { store.dimensions }
        onChange = { v => runInAction(() => store.mergeDimension1Index = v) }
        optionToMenuItemMapper = { (dimension, index) => (
          <MenuItem key = { 'merge-select-dimension-1-' + index } value = {index}>{dimension.name}</MenuItem>
        ) }
      />
      <SingleSelectWithLabel 
        id = 'merge-select-dimension-2'
        label = 'Dimension 2'
        value = { store.mergeDimension2Index }
        options = { store.dimensions }
        onChange = { v => runInAction(() => store.mergeDimension2Index = v) }
        optionToMenuItemMapper = { (dimension, index) => (
          <MenuItem key = { 'merge-select-dimension-2-' + index } value = {index}>{dimension.name}</MenuItem>
        ) }
      />
    </div>
    <div>
      <TextField
        id = 'new-cube-name-text-field'
        label = 'Name of new cube'
        style = {{ marginBottom: 5 }}
        value = { store.newCubeName }
        onChange = { e => runInAction(() => store.newCubeName = e.target.value) }
        size = 'small'
      />
      <Button onClick = {() => {
        // TODO: Query backend, show new name
        setComplete(true);
        runInAction(() => store.newCubeName = 'sales-merged')
      }}>Transform</Button>
    </div>
    <div style = {{ display: isComplete ? 'flex' : 'none' }}>
      <CheckCircleOutlineIcon/>Cube saved as {store.newCubeName}
    </div>
  </div> );
})

const CompactTableCell = observer(({children}: {children: ReactNode}) => (
  <TableCell sx = {{ paddingTop: 1, paddingBottom: 1 }}>{children}</TableCell>
));

const SingleSelectWithLabel = observer(({ id, label, value, options, onChange, optionToMenuItemMapper }: {
  id: string,
  label: string,
  value: any,
  options: any[],
  onChange: (value: any) => void,
  optionToMenuItemMapper: (option: any, index: number) => ReactNode
}) => (
  <FormControl sx = {{ minWidth: 200 }}>
    <InputLabel htmlFor = {id}>{label}</InputLabel>
    <Select
      id = {id} label = {label}
      style = {{ marginBottom: 10, marginRight: 10 }}
      size = 'small'
      value = { value }
      onChange = { e => onChange(e.target.value as number) }>
      { options.map(optionToMenuItemMapper) }
    </Select>
  </FormControl>
));

const CharInSquare = observer(({ char, emphasize } : { char: string, emphasize: boolean }) => (
  <div style = {{
    display: 'inline-block',
    width: 20, height: 20,
    marginLeft: 5,
    borderWidth: emphasize ? 3 : 1, borderStyle: 'solid',
    textAlign: 'center', verticalAlign: 'middle', lineHeight: emphasize ? '16px' : '20px'
  }}>
    {char}
  </div>
));
