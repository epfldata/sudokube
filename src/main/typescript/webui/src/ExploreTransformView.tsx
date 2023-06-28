import { Button, Container, Dialog, DialogActions, DialogContent, DialogTitle, FormControl, InputLabel, MenuItem, Select, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, TextField } from "@mui/material";
import { observer } from "mobx-react-lite";
import React, { ReactNode, useEffect, useState } from "react";
import { apiBaseUrl } from "./configs";
import { useRootStore } from "./RootStore";
import CheckCircleOutlineIcon from '@mui/icons-material/CheckCircleOutline';
import CancelIcon from '@mui/icons-material/Cancel';
import { runInAction } from "mobx";
import { SudokubeService } from "./_proto/sudokubeRPC_pb_service";
import { grpc } from "@improbable-eng/grpc-web";
import { Empty, GetCubesResponse, GetRenameTimeArgs, GetRenameTimeResponse, IsRenamedQueryArgs, IsRenamedQueryResponse, MergeColumnDef, SelectDataCubeArgs, SelectDataCubeForExploreResponse, TransformDimensionsArgs } from "./_proto/sudokubeRPC_pb";
import { buildMessage } from "./Utils";

export default observer(function ExploreTransformView() {
  const { exploreTransformStore: store, errorStore } = useRootStore();
  useEffect(() => {
    grpc.unary(SudokubeService.getDataCubesForExplore, {
      host: apiBaseUrl,
      request: new Empty(),
      onEnd: response => {
        if (response.status !== 0) {
          runInAction(() => {
            errorStore.errorMessage = response.statusMessage;
            errorStore.isErrorPopupOpen = true;
          });
          return;
        }
        runInAction(() => {
          store.cubes = (response.message as GetCubesResponse)?.getCubesList();
          store.cube = store.cubes[0];
        });
        grpc.unary(SudokubeService.selectDataCubeForExplore, {
          host: apiBaseUrl,
          request: buildMessage(new SelectDataCubeArgs(), {cube: store.cube}),
          onEnd: response => {
            if (response.status !== 0) {
              runInAction(() => {
                errorStore.errorMessage = response.statusMessage;
                errorStore.isErrorPopupOpen = true;
              });
              return;
            }
            runInAction(() => {
              store.dimensions = (response.message as SelectDataCubeForExploreResponse)?.getDimNamesList();
            })
          }
        })
      }
    });
  }, []);
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
  const { exploreTransformStore: store, errorStore } = useRootStore();
  return ( <div>
    <FormControl sx = {{ minWidth: 200 }}>
      <InputLabel htmlFor = "select-cube">Select Cube</InputLabel>
      <Select
        id = "select-cube" label = "Select Cube"
        style = {{ marginBottom: 10 }}
        size = 'small'
        value = {store.cube}
        onChange = { e => {
          runInAction(() => {
            store.cube = e.target.value;
            grpc.unary(SudokubeService.selectDataCubeForExplore, {
              host: apiBaseUrl,
              request: buildMessage(new SelectDataCubeArgs(), {cube: store.cube}),
              onEnd: response => {
                if (response.status !== 0) {
                  runInAction(() => {
                    errorStore.errorMessage = response.statusMessage;
                    errorStore.isErrorPopupOpen = true;
                  });
                  return;
                }
                runInAction(() => {
                  store.dimensions = (response.message as SelectDataCubeForExploreResponse)?.getDimNamesList();
                })
              }
            })
          })
        } }>
        { store.cubes.map(cube => (
          <MenuItem key = { 'select-cube-' + cube } value = {cube}>{cube}</MenuItem>
        )) }
      </Select>
    </FormControl>
  </div> );
})

const CheckRename = observer(() => {
  const { exploreTransformStore: store, errorStore } = useRootStore();
  const [dimension1, setDimension1] = useState(store.dimensions[0]);
  const [dimension2, setDimension2] = useState(store.dimensions[1]);
  const [dimensionsNullCounts, setDimensionsNullCounts] = useState([0, 0, 0, 0]);
  const [isRenamed, setRenameCheckResult] = useState(false);
  const [isRenameCheckResultShown, setRenameCheckResultShown] = useState(false);

  useEffect(() => {
    setDimension1(store.dimensions[0]);
    setDimension2(store.dimensions[1]);
    setRenameCheckResultShown(false);
  }, [store.dimensions]);

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
          <MenuItem key = { 'check-rename-select-dimension-1-' + dimension } value = {dimension}>{dimension}</MenuItem>
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
            key = { 'check-rename-select-dimension-2-' + dimension }
            value = {dimension}
          >
            {dimension}
          </MenuItem>
        ) }
      />
      <Button onClick = {() => {
        grpc.unary(SudokubeService.isRenameQuery, {
          host: apiBaseUrl,
          request: buildMessage(new IsRenamedQueryArgs(), {
            dimension1: dimension1,
            dimension2: dimension2
          }),
          onEnd: response => {
            if (response.status !== 0) {
              runInAction(() => {
                errorStore.errorMessage = response.statusMessage;
                errorStore.isErrorPopupOpen = true;
              });
              return;
            }
            const { resultList: nullCounts, isRenamed } = (response.message as IsRenamedQueryResponse).toObject();
            setDimensionsNullCounts(nullCounts);
            setRenameCheckResult(isRenamed);
            setRenameCheckResultShown(true);
          }
        })
      }}>Check</Button>
    </div>
  ));

  const ResultTable = observer(() => (
    <TableContainer>
      <Table sx = {{ width: 'auto', }} aria-label = "simple table">
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
            <CompactTableCell variant = 'head'>Not NULL</CompactTableCell>
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
  const { exploreTransformStore: store, errorStore } = useRootStore();

  const [dimension, setDimension] = useState(store.dimensions[0] ?? '');
  const [isStepShown, setStepShown] = useState(false);
  const [numTimeBits, setNumTimeBits] = useState(0);
  const [filters, setFilters] = useState<boolean[]>([]);
  useEffect(() => {
    setDimension(store.dimensions[0] ?? '');
    setStepShown(false);
    setFilters([]);
  }, [store.dimensions]);

  type TimeRangeNullCountsRow = {
    timeRange: string;
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
    let chars: string[] = filters.map(bit => bit ? '1' : '0');
    if (filters.length < numTimeBits) {
      chars.push('?');
    }
    const remainingBits = numTimeBits - filters.length - 1;
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
          <MenuItem key = { 'rename-time-select-dimension-' + dimension } value = {dimension}>{dimension}</MenuItem>
        ) }
      />
      <Button onClick = {() => {
        grpc.unary(SudokubeService.startRenameTimeQuery, {
          host: apiBaseUrl,
          request: buildMessage(new GetRenameTimeArgs, {dimensionName: dimension}),
          onEnd: response => {
            if (response.status !== 0) {
              runInAction(() => {
                errorStore.errorMessage = response.statusMessage;
                errorStore.isErrorPopupOpen = true;
              });
              return;
            }
            const {
              numTimeBits,
              isComplete,
              filtersAppliedList,
              resultRowsList,
              renameTime
            } = (response.message as GetRenameTimeResponse).toObject();
            setNumTimeBits(numTimeBits);
            setFilters(filtersAppliedList);
            setNullCounts(resultRowsList);
            setSearchEnded(isComplete);
            setStepShown(true);
            if (isComplete) {
              setRenameTime(renameTime);
            }
          }
        })
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
                <CompactTableCell>{row.timeRange}</CompactTableCell>
                <CompactTableCell>{row.nullCount}</CompactTableCell>
                <CompactTableCell>{row.notNullCount}</CompactTableCell>
              </TableRow>
            )) }
          </TableBody>
        </Table>
      </TableContainer>

      <Button 
        disabled = {isSearchEnded} 
        onClick = {() => {
          grpc.unary(SudokubeService.continueRenameTimeQuery, {
            host: apiBaseUrl,
            request: new Empty(),
            onEnd: response => {
              if (response.status !== 0) {
                runInAction(() => {
                  errorStore.errorMessage = response.statusMessage;
                  errorStore.isErrorPopupOpen = true;
                });
                return;
              }
              const {
                numTimeBits,
                isComplete,
                filtersAppliedList,
                resultRowsList,
                renameTime
              } = (response.message as GetRenameTimeResponse).toObject();
              setNumTimeBits(numTimeBits);
              setFilters(filtersAppliedList);
              setNullCounts(resultRowsList);
              setSearchEnded(isComplete);
              setStepShown(true);
              if (isComplete) {
                setRenameTime(renameTime);
              }
            }
          })
        }}>
        Continue
      </Button>
    </div>
    <div hidden = {!isSearchEnded}>{renameTime}</div>
  </div> );
})

const Transform = observer(() => {
  const { exploreTransformStore: store, errorStore } = useRootStore();
  const [newCubeName, setNewCubeName] = useState('');
  const [isComplete, setComplete] = useState(false);

  useEffect(() => {
    runInAction(() => store.transformations = [
      {
        dim1: store.dimensions[0],
        dim2: store.dimensions[1],
        newDim: ''
      }
    ]);
    setNewCubeName('');
    setComplete(false);
  }, [store.dimensions]);

  const IndividualTransformation = observer(({transformation, index}: {
    transformation: MergeColumnDef.AsObject, index: number
  }) => (
    <div>
      <SingleSelectWithLabel
        id = {'transform-select-' + index}
        label = 'Transformation'
        value = {'Merge'}
        options = { ['Merge'] }
        onChange = { () => {} }
        optionToMenuItemMapper = { (transformation) => (
          <MenuItem key = { 'transform-select-' + index } value = 'Merge'>{transformation}</MenuItem>
        ) }
      />
      <SingleSelectWithLabel 
        id = {'merge-' + index + '-select-dimension-1'}
        label = 'Dimension 1'
        value = { transformation.dim1 }
        options = { store.dimensions }
        onChange = { v => transformation.dim1 = v }
        optionToMenuItemMapper = { dimension => (
          <MenuItem key = { 'merge' + index + '-select-dimension-1-' + dimension } value = {dimension}>{dimension}</MenuItem>
        ) }
      />
      <SingleSelectWithLabel 
        id = {'merge-' + index + '-select-dimension-2'}
        label = 'Dimension 2'
        value = { transformation.dim2 }
        options = { store.dimensions }
        onChange = { v => transformation.dim2 = v }
        optionToMenuItemMapper = { dimension => (
          <MenuItem key = { 'merge' + index + '-select-dimension-2-' + dimension } value = {dimension}>{dimension}</MenuItem>
        ) }
      />
      <TextField
        id = { 'merge-' + index + '-new-dimension-name-text-field' }
        label = 'Name of merged dimension'
        style = {{ marginBottom: 5 }}
        value = { transformation.newDim }
        onChange = { e => transformation.newDim = e.target.value }
        size = 'small'
      />
    </div>
  ));

  return ( <div>
    <h3>Transform</h3>
    { store.transformations.map((transformation, index) => (
      <IndividualTransformation transformation={transformation} index={index}/>
    )) }
    <div>
      <Button onClick = {() => runInAction(() => {
        store.transformations.push(
          {
            dim1: store.dimensions[0],
            dim2: store.dimensions[1],
            newDim: ''
          }
        );
      })}>Add transformation</Button>
    </div>
    <TextField
      id = 'new-cube-name-text-field'
      label = 'Name of new cube'
      style = {{ marginBottom: 5 }}
      value = { newCubeName }
      onChange = { e => setNewCubeName(e.target.value) }
      size = 'small'
    />
    <Button onClick = {() => {
      grpc.unary(SudokubeService.transformCube, {
        host: apiBaseUrl,
        request: buildMessage(new TransformDimensionsArgs(), {
          colsList: store.transformations.map(transformation => buildMessage(new MergeColumnDef(), transformation)),
          newCubeName: newCubeName
        }),
        onEnd: response => {
          if (response.status !== 0) {
            runInAction(() => {
              errorStore.errorMessage = response.statusMessage;
              errorStore.isErrorPopupOpen = true;
            });
            return;
          }
          setComplete(true)
        }
      });
    }}>Transform</Button>
    <Dialog open = {isComplete}>
      <DialogTitle>Success</DialogTitle>
      <DialogContent>Cube saved as {newCubeName}</DialogContent>
      <DialogActions>
        <Button onClick = { () => setComplete(false) }>OK</Button>
      </DialogActions>
    </Dialog>
  </div> );
});

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

