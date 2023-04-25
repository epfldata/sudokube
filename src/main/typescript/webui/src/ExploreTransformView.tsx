import { Button, Container, FormControl, InputLabel, MenuItem, Select, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, TextField } from "@mui/material";
import { observer } from "mobx-react-lite";
import React, { ReactNode, useState } from "react";
import { useRootStore } from "./RootStore";
import CheckCircleOutlineIcon from '@mui/icons-material/CheckCircleOutline';
import CancelIcon from '@mui/icons-material/Cancel';
import { runInAction } from "mobx";

export default observer(function ExploreTransformView() {
  const { exploreTransformStore: store } = useRootStore();
  runInAction(() => {
    // TODO: Call backend to fetch these things for the default cube
    store.dimensions = [
      { name: 'Country', numBits: 6 }, 
      { name: 'City', numBits: 6 }, 
      { name: 'Year', numBits: 6 }, 
      { name: 'Month', numBits: 5 }, 
      { name: 'Day', numBits: 6 }
    ];
    store.timeNumBits = 16;
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
  // TODO: Call backend to fetch cubes
  const [cubes, setCubes] = useState(['sales']);
  const [cube, setCube] = useState(cubes[0]);
  return ( <div>
    <FormControl sx = {{ minWidth: 200 }}>
      <InputLabel htmlFor = "select-cube">Select Cube</InputLabel>
      <Select
        id = "select-cube" label = "Select Cube"
        style = {{ marginBottom: 10 }}
        size = 'small'
        value = {cube}
        onChange = { e => {
          setCube(e.target.value);
          // TODO: Load dimensions and number of time bits
          runInAction(() => {
            // TODO: Call backend to fetch these things
            store.dimensions = [
              { name: 'Country', numBits: 6 }, 
              { name: 'City', numBits: 6 }, 
              { name: 'Year', numBits: 6 }, 
              { name: 'Month', numBits: 5 }, 
              { name: 'Day', numBits: 6 }
            ];
            store.timeNumBits = 16;
          })
        } }>
        { cubes.map(cube => (
          <MenuItem key = { 'select-cube-' + cube } value = {cube}>{cube}</MenuItem>
        )) }
      </Select>
    </FormControl>
  </div> );
})

const CheckRename = observer(() => {
  const { exploreTransformStore: store } = useRootStore();
  const [dimension1, setDimension1] = useState(store.dimensions[0].name);
  const [dimension2, setDimension2] = useState(store.dimensions[1].name);
  const [dimensionsNullCounts, setDimensionsNullCounts] = useState([0, 0, 0, 0]);
  const [isRenamed, setRenameCheckResult] = useState(false);
  const [isRenameCheckResultShown, setRenameCheckResultShown] = useState(false);

  const SelectDimensions = observer(() => (
    <div>
      <SingleSelectWithLabel 
        id = 'check-rename-select-dimension-1'
        label = 'Dimension 1'
        value = {dimension1}
        options = {store.dimensions}
        onChange = { v => {
          setDimension1(v);
          setRenameCheckResultShown(false);
        }}
        optionToMenuItemMapper = { dimension => (
          <MenuItem key = { 'check-rename-select-dimension-1-' + dimension.name } value = {dimension.name}>{dimension.name}</MenuItem>
        ) }
      />
      <SingleSelectWithLabel 
        id = 'check-rename-select-dimension-2'
        label = 'Dimension 2'
        value = {dimension2}
        options = {store.dimensions}
        onChange = { v => {
          setDimension2(v);
          setRenameCheckResultShown(false);
        }}
        optionToMenuItemMapper = { dimension => (
          <MenuItem
            key = { 'check-rename-select-dimension-2-' + dimension.name }
            value = {dimension.name}
          >
            {dimension.name}
          </MenuItem>
        ) }
      />
      <Button onClick = {() => {
        // TODO: Query backend and show results
        setDimensionsNullCounts([0, 0, 0, 0]);
        setRenameCheckResult(true);
        setRenameCheckResultShown(true);
      }}>Check</Button>
    </div>
  ));

  const ResultTable = observer(() => (
    <TableContainer>
      <Table sx = {{ width: 'auto' }} aria-label = "simple table">
        <TableHead>
          <TableRow>
            <CompactTableCell>{}</CompactTableCell>
            <CompactTableCell>{}</CompactTableCell>
            <CompactTableCell align = 'center' colSpan = {2}>{dimension2}</CompactTableCell>
          </TableRow>
          <TableRow>
            <CompactTableCell>{}</CompactTableCell>
            <CompactTableCell>{}</CompactTableCell>
            <CompactTableCell>NULL</CompactTableCell>
            <CompactTableCell>Not NULL</CompactTableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          <TableRow>
            <CompactTableCell align = 'center' rowSpan = {2} variant = 'head'>{dimension1}</CompactTableCell>
            <CompactTableCell variant = 'head'>NULL</CompactTableCell>
            <CompactTableCell>{dimensionsNullCounts[0]}</CompactTableCell>
            <CompactTableCell>{dimensionsNullCounts[1]}</CompactTableCell>
          </TableRow>
          <TableRow>
            <CompactTableCell variant = 'head'>NOT NULL</CompactTableCell>
            <CompactTableCell>{dimensionsNullCounts[2]}</CompactTableCell>
            <CompactTableCell>{dimensionsNullCounts[3]}</CompactTableCell>
          </TableRow>
        </TableBody>
      </Table>
    </TableContainer>
  ));

  const ResultMessage = observer(() => {
    if (isRenamed) {
      return (
        <span style = {{display: 'flex'}}>
          <CheckCircleOutlineIcon/>
          {dimension1} is likely renamed to {dimension2}.
        </span>
      )
    } else {
      return (
        <span style = {{display: 'flex'}}>
          <CancelIcon/>
          {dimension1} is not renamed to {dimension2}.
        </span>
      )
    }
  });

  return ( <div>
    <h3>Check potentially renamed dimensions pair</h3>
    <SelectDimensions/>
    <div hidden = {!isRenameCheckResultShown}>
      <ResultTable/>
      <ResultMessage/>
    </div>
  </div> );
})

const FindRenameTime = observer(() => {
  const { exploreTransformStore: store } = useRootStore();

  const [dimension, setDimension] = useState(store.dimensions[0].name);

  const [isStepShown, setStepShown] = useState(false);

  const [filters, setFilters] = useState<(0|1)[]>([]);

  type TimeRangeNullCountsRow = {
    range: string;
    nullCount: number;
    notNullCount: number;
  }
  const [nullCounts, setNullCounts] = useState<TimeRangeNullCountsRow[]>([]);

  const [isSearchEnded, setSearchEnded] = useState(false);
  const [renameTime, setRenameTime] = useState('...');

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

  const BitsInSquares = observer(() => {
    let chars: string[] = filters.map(bit => bit.toString());
    if (filters.length < store.timeNumBits) {
      chars.push('?');
    }
    const remainingBits = store.timeNumBits - filters.length - 1;
    if (remainingBits > 0) {
      chars = chars.concat(Array(remainingBits).fill('✕'));
    }
    return (<span>
      { chars.map(char => <CharInSquare char = { char } emphasize = { char === '?' } />) }
    </span>)
  });

  return ( <div>
    <h3>Find time of rename</h3>

    <div>
      <SingleSelectWithLabel
        id = 'rename-time-select-dimension'
        label = 'Dimension'
        value = { dimension }
        options = { store.dimensions }
        onChange = { v => {
          setDimension(v);
          setStepShown(false);
          setSearchEnded(false);
          setFilters([]);
        }}
        optionToMenuItemMapper = { dimension => (
          <MenuItem key = { 'check-rename-select-dimension-2-' + dimension.name } value = {dimension.name}>{dimension.name}</MenuItem>
        ) }
      />
      <Button onClick = {() => {
        // TODO: Query the first bit
        setFilters([]);
        setSearchEnded(false);
        setNullCounts([
          { range: '0 to 31', nullCount: 0, notNullCount: 0 },
          { range: '32 to 63', nullCount: 0, notNullCount: 0 }
        ]);
        setStepShown(true);
      }}>
        Start
      </Button>
    </div>

    <div hidden = {!isStepShown}>
      <div>Filters on time bits applied: <BitsInSquares/> </div>
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
            { nullCounts.map(row => (
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
        setFilters(filters.slice().concat(0));
        setSearchEnded(true);
        setRenameTime('...');
      }}>
      Continue
    </Button>
    <div hidden = {!isSearchEnded}>Dimension is renamed at {renameTime}</div>
  </div> );
})

const Transform = observer(() => {
  const [transformation, setTransformation] = useState('Merge');
  const transformationDetails = () => {
    switch (transformation) {
      case 'Merge':
        return <Merge/>;
    }
  }
  return ( <div>
    <h3>Transform</h3>
    <div>
      <SingleSelectWithLabel
        id = 'transform-select'
        label = 'Transformation'
        value = {0}
        options = { ['Merge'] }
        onChange = { e => setTransformation(e.target.value) }
        optionToMenuItemMapper = { (transformation, index) => (
          <MenuItem key = { 'transform-select-' + index } value = {index}>{transformation}</MenuItem>
        ) }
      />
    </div>
    { transformationDetails() }
  </div> );
})

const Merge = observer(() => {
  const { exploreTransformStore: store } = useRootStore();
  const [dimension1Index, setDimension1Index] = useState(0);
  const [dimension2Index, setDimension2Index] = useState(0);
  const [mergedDimensionName, setMergedDimensionName] = useState('');
  const [newCubeName, setNewCubeName] = useState('');
  const [isComplete, setComplete] = useState(false);
  return <div>
    <div>
      <SingleSelectWithLabel 
        id = 'merge-select-dimension-1'
        label = 'Dimension 1'
        value = { dimension1Index }
        options = { store.dimensions }
        onChange = { v => setDimension1Index(v) }
        optionToMenuItemMapper = { (dimension, index) => (
          <MenuItem key = { 'merge-select-dimension-1-' + index } value = {index}>{dimension.name}</MenuItem>
        ) }
      />
      <SingleSelectWithLabel 
        id = 'merge-select-dimension-2'
        label = 'Dimension 2'
        value = { dimension2Index }
        options = { store.dimensions }
        onChange = { v => setDimension2Index(v) }
        optionToMenuItemMapper = { (dimension, index) => (
          <MenuItem key = { 'merge-select-dimension-2-' + index } value = {index}>{dimension.name}</MenuItem>
        ) }
      />
      <TextField
        id = 'new-dimension-name-text-field'
        label = 'Name of merged dimension'
        style = {{ marginBottom: 5 }}
        value = { mergedDimensionName }
        onChange = { e => setMergedDimensionName(e.target.value) }
        size = 'small'
      />
    </div>
    <div>
      <TextField
        id = 'new-cube-name-text-field'
        label = 'Name of new cube'
        style = {{ marginBottom: 5 }}
        value = { newCubeName }
        onChange = { e => setNewCubeName(e.target.value) }
        size = 'small'
      />
      <Button onClick = {() => {
        // TODO: Query backend, show new name
        setComplete(true);
      }}>Transform</Button>
    </div>
    <div style = {{ display: isComplete ? 'flex' : 'none' }}>
      <CheckCircleOutlineIcon/>Cube saved as {newCubeName}
    </div>
  </div>
})

const CompactTableCell = observer(({children, align, colSpan, rowSpan, variant}: {
  children: ReactNode,
  align?: 'center',
  colSpan?: number, rowSpan?: number,
  variant?: 'head' | undefined
}) => (
  <TableCell
    sx = {{ paddingTop: 1, paddingBottom: 1 }}
    align = {align}
    colSpan = {colSpan} rowSpan={rowSpan}
    variant = {variant}
  >
    {children}
  </TableCell>
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

