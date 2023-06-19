import * as React from 'react';
import Container from '@mui/material/Container';
import Grid from '@mui/material/Grid';
import { FilterChip } from './QueryViewChips';
import Chip from '@mui/material/Chip';
import FilterAltIcon from '@mui/icons-material/FilterAlt';
import { Box, Button, Dialog, DialogActions, DialogContent, DialogTitle, FormControl, FormLabel, IconButton, InputLabel, MenuItem, Select, TextField, Tooltip, useTheme } from '@mui/material';
import { observer } from 'mobx-react-lite';
import { apiBaseUrl, useRootStore } from './RootStore';
import MaterializationStore, { MaterializationDimension } from './MaterializationStore';
import { ButtonChip, chipStyle } from './GenericChips';
import { runInAction } from 'mobx';
import { useEffect, useState } from 'react';
import {grpc} from "@improbable-eng/grpc-web";
import {SudokubeService} from "./_proto/sudokubeRPC_pb_service";
import {Empty, BaseCuboidResponse, SelectBaseCuboidArgs, SelectBaseCuboidResponse, SelectMaterializationStrategyArgs, GetCuboidsArgs, DimensionFilterCuboid, GetChosenCuboidsResponse, CuboidDef, MaterializeArgs, DeleteSelectedCuboidArgs, GetAvailableCuboidsResponse, ManuallyUpdateCuboidsArgs} from "./_proto/sudokubeRPC_pb";
import { buildMessage } from './Utils';
import MaterialReactTable, { MRT_ColumnDef, MRT_RowSelectionState } from 'material-react-table';
import { Delete } from '@mui/icons-material';
import { ErrorStore } from './ErrorStore';

const fetchChosenCuboids = (store: MaterializationStore, errorStore: ErrorStore) => {
  grpc.unary(SudokubeService.getChosenCuboids, {
    host: apiBaseUrl,
    request: buildMessage(new GetCuboidsArgs(), {
      filtersList: store.chosenCuboidsFilters,
      requestedPageId: store.chosenCuboidsPage,
      rowsPerPage: store.chosenCuboidsPageSize
    }),
    onEnd: response => {
      if (response.status !== 0) {
        runInAction(() => {
          errorStore.errorMessage = response.statusMessage;
          errorStore.isErrorPopupOpen = true;
        });
        return;
      }
      runInAction(() => {
        store.chosenCuboids = (response.message as GetChosenCuboidsResponse)!.getCuboidsList();
      })
    }
  });
}

export default observer(function Materialization() {
  const { materializationStore: store, errorStore } = useRootStore();
  const [cubeName, setCubeName] = useState('');
  const [isSuccessDialogOpen, setSuccessDialogOpen] = useState(false);

  useEffect(() => {
    grpc.unary(SudokubeService.getBaseCuboids, {
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
          store.datasets = (response.message as BaseCuboidResponse)!.toObject().cuboidsList;
          store.selectedDataset = store.datasets[0];
        });
        grpc.unary(SudokubeService.selectBaseCuboid, {
          host: apiBaseUrl,
          request: buildMessage(new SelectBaseCuboidArgs(), { cuboid: store.selectedDataset }),
          onEnd: response => {
            if (response.status !== 0) {
              runInAction(() => {
                errorStore.errorMessage = response.statusMessage;
                errorStore.isErrorPopupOpen = true;
              });
              return;
            }
            runInAction(() => {
              store.dimensions = (response.message as SelectBaseCuboidResponse)!.toObject().dimensionsList;
            })
          }
        });
      }
    });
  }, []);

  useEffect(() => setCubeName(''), [store.selectedDataset]);

  runInAction(() => {
    store.strategies = [
      {
        name: 'Prefix', 
        parameters: [
          { name: 'logN' },
          { name: 'minD' }
        ]
      },
      {
        name: 'Randomized', 
        parameters: [
          { name: 'logN' },
          { name: 'minD' }
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
        <ManuallyChooseCuboids/>
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
          grpc.unary(SudokubeService.materializeCuboids, {
            host: apiBaseUrl,
            request: buildMessage(new MaterializeArgs(), { cubeName: cubeName }),
            onEnd: response => {
              if (response.status !== 0) {
                runInAction(() => {
                  errorStore.errorMessage = response.statusMessage;
                  errorStore.isErrorPopupOpen = true;
                });
                return;
              }
              runInAction(() => setSuccessDialogOpen(true))
            }
          })
        } }>Materialize</Button>
        <Dialog open = {isSuccessDialogOpen}>
          <DialogTitle>Success</DialogTitle>
          <DialogContent>Cube saved as {cubeName}</DialogContent>
          <DialogActions>
            <Button onClick = { () => setSuccessDialogOpen(false) }>OK</Button>
          </DialogActions>
        </Dialog>
      </div>
    </Container>
  )
})

const SelectDataset = observer(() => {
  const { materializationStore: store, errorStore } = useRootStore();
  return ( <div>
    <FormControl sx = {{ minWidth: 200 }}>
      <InputLabel htmlFor = "select-dataset">Dataset</InputLabel>
      <Select
        id = "select-dataset" label = "Dataset"
        style = {{ maxHeight: '50px', marginBottom: '5px' }}
        value = { store.selectedDataset }
        onChange = { e => runInAction(() => {
            store.selectedDataset = e.target.value;
            const selectBaseCuboidArgs = new SelectBaseCuboidArgs();
            selectBaseCuboidArgs.setCuboid(store.selectedDataset);
            grpc.unary(SudokubeService.selectBaseCuboid, {
              host: apiBaseUrl,
              request: selectBaseCuboidArgs,
              onEnd: response => {
                if (response.status !== 0) {
                  runInAction(() => {
                    errorStore.errorMessage = response.statusMessage;
                    errorStore.isErrorPopupOpen = true;
                  });
                  return;
                }
                runInAction(() => {
                  store.chosenCuboidsFilters = [];
                  store.dimensions = (response.message as SelectBaseCuboidResponse)!.toObject().dimensionsList;
                })
              }
            });
        })}>
        { store.datasets.map((dataset, index) => (
          <MenuItem key = { 'select-dataset-' + index } value = {dataset}>{dataset}</MenuItem>
        )) }
      </Select>
    </FormControl>
  </div> );
})

const SpecifyStrategy = observer(() => {
  const { materializationStore: store, errorStore } = useRootStore();
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
            grpc.unary(SudokubeService.selectMaterializationStrategy, {
              host: apiBaseUrl,
              request: buildMessage(new SelectMaterializationStrategyArgs(), {
                name: store.strategies[store.strategyIndex].name,
                argsList: store.strategyParameters
              }),
              onEnd: response => {
                if (response.status !== 0) {
                  runInAction(() => {
                    errorStore.errorMessage = response.statusMessage;
                    errorStore.isErrorPopupOpen = true;
                  });
                  return;
                }
                setDialogOpen(false);
                fetchChosenCuboids(store, errorStore);
              }
            });
          } }>Confirm</Button>
        </DialogActions>
      </DialogContent>
    </Dialog>
  </span> );
})

const ManuallyChooseCuboids = observer(() => {
  const { materializationStore: store, errorStore } = useRootStore();
  const dimensions = store.dimensions;
  const [isCuboidsDialogOpen, setCuboidsDialogOpen] = React.useState(false);
  const [page, setPage] = useState(0), [pageSize, setPageSize] = useState(10);
  const [cuboids, setCuboids] = useState<CuboidDef[]>([]);
  const [rowSelectionModel, setRowSelectionModel] = useState<MRT_RowSelectionState>({});

  const fetchAvailableCuboids = ({newPage, newPageSize} : {
    newPage?: number, newPageSize?: number
  }) => {
    grpc.unary(SudokubeService.getAvailableCuboids, {
      host: apiBaseUrl,
      request: buildMessage(new GetCuboidsArgs(), {
        filtersList: store.addCuboidsFilters,
        requestedPageId: newPage ?? page,
        rowsPerPage: newPageSize ?? pageSize
      }),
      onEnd: res => {
        if (res.status !== 0) {
          runInAction(() => {
            errorStore.errorMessage = res.statusMessage;
            errorStore.isErrorPopupOpen = true;
          });
          return;
        }
        const rpcCuboidsList = (res.message as GetAvailableCuboidsResponse)!.getCuboidsList();
        setCuboids(rpcCuboidsList.map(cuboid => buildMessage(new CuboidDef(), {
          dimensionsList: cuboid.getDimensionsList()
        })));
        const newRowSelectionModel: MRT_RowSelectionState = {};
        rpcCuboidsList.forEach((cuboid, index) => { newRowSelectionModel[String(index)] = cuboid.getIsChosen() });
        setRowSelectionModel(newRowSelectionModel);
      }
    });
  }

  useEffect(() => {
    runInAction(() => store.addCuboidsFilters = []);
  }, [store.selectedDataset]);

  useEffect(() => {
    if (isCuboidsDialogOpen) {
      fetchAvailableCuboids({});
    }
  }, [store.selectedDataset, isCuboidsDialogOpen])

  return ( <span>
    <ButtonChip label = 'Manually choose cuboids' variant = 'outlined' onClick = { () => setCuboidsDialogOpen(true) } />
    <Dialog fullScreen open = {isCuboidsDialogOpen}>
      <DialogTitle>Choose cuboids</DialogTitle>
      <DialogContent>
        <Grid container maxHeight='30vh' style = {{ paddingTop: '1px', paddingBottom: '1px' }}>
          <Grid item xs={6}>
            { store.addCuboidsFilters.map((filter, index) => {
              return (<FilterChip
                key = { 'materialization-choose-cuboids-filter-chip-' + filter.getDimensionName() + '-' + filter.getBitsFrom() + '-' + filter.getBitsTo() }
                text = { filter.getDimensionName() + ' / ' + filter.getBitsFrom() + '–' + filter.getBitsTo() }
                onDelete = { () => {
                  runInAction(() => store.addCuboidsFilters.splice(index, 1));
                  fetchAvailableCuboids({});
                } }
              /> );
            }) }
            <AddCuboidsFilterChip onAdd = {(dimensionIndex: number, bitsFrom: number, bitsTo: number) => {
              store.addCuboidsFilters.push(
                buildMessage(new DimensionFilterCuboid(), {
                  dimensionName: dimensions[dimensionIndex].name,
                  bitsFrom: bitsFrom,
                  bitsTo: bitsTo
                })
              );
              fetchAvailableCuboids({});
            }} />
          </Grid>
        </Grid>

        <div style = {{ marginTop: 20 }}>
          <MaterialReactTable
            columns = { store.dimensions.map(dimensionToColumn) }
            enableColumnResizing
            data = { cuboids.map(cuboidToRow) } 
            getRowId = { row => row.id }
            enableRowSelection
            rowCount = {Number.MAX_VALUE}
            enablePagination
            manualPagination
            muiTablePaginationProps = {{
              showFirstButton: false,
              showLastButton: false
            }}
            state = {{
              density: 'compact',
              pagination: { pageIndex: page, pageSize: pageSize },
              rowSelection: rowSelectionModel
            }}
            onRowSelectionChange={setRowSelectionModel}
            onPaginationChange={(updater: any) => {
              const model = updater({pageIndex: page, pageSize: pageSize});
              grpc.unary(SudokubeService.manuallyUpdateCuboids, {
                host: apiBaseUrl,
                request: buildMessage(new ManuallyUpdateCuboidsArgs(), {
                  isChosenList: Array.from(cuboids.keys()).map(i => rowSelectionModel[String(i)])
                }),
                onEnd: response => {
                  if (response.status !== 0) {
                    runInAction(() => {
                      errorStore.errorMessage = response.statusMessage;
                      errorStore.isErrorPopupOpen = true;
                    });
                    return;
                  }
                  setPage(model.pageIndex);
                  setPageSize(model.pageSize);
                  fetchAvailableCuboids({newPage: model.pageIndex, newPageSize: model.pageSize});
                }
              });
            }}
            muiBottomToolbarProps = {{
              sx: {
                '.MuiTablePagination-displayedRows': { display: 'none' },
                '.MuiTablePagination-selectLabel': { display: 'none' },
                '.MuiTablePagination-select': { display: 'none' },
                '.MuiTablePagination-selectIcon': { display: 'none' }
              } 
            }}
            renderTopToolbar = {false}
          />
        </div>

        <DialogActions>
          <Button onClick = { () => setCuboidsDialogOpen(false) }>Cancel</Button>
          <Button onClick = { () => {
            grpc.unary(SudokubeService.manuallyUpdateCuboids, {
              host: apiBaseUrl,
              request: buildMessage(new ManuallyUpdateCuboidsArgs(), {
                isChosenList: Array.from(cuboids.keys()).map(i => rowSelectionModel[String(i)])
              }),
              onEnd: response => {
                if (response.status !== 0) {
                  runInAction(() => {
                    errorStore.errorMessage = response.statusMessage;
                    errorStore.isErrorPopupOpen = true;
                  });
                  return;
                }
                setCuboidsDialogOpen(false); 
                fetchChosenCuboids(store, errorStore);
              }
            });
          } }>Confirm</Button>
        </DialogActions>
      </DialogContent>
    </Dialog>
  </span> );
});

const ChosenCuboids = observer(() => {
  const { materializationStore: store, errorStore } = useRootStore();

  useEffect(() => {
    runInAction(() => {
      store.chosenCuboidsFilters = [];
      store.chosenCuboidsPage = 0;
    });
    fetchChosenCuboids(store, errorStore);
  }, [store.selectedDataset]);

  return ( <div>
    <Grid container maxHeight='30vh' style={{ paddingTop: '1px', paddingBottom: '1px' }}>
      <Grid item xs={6}>
        { store.chosenCuboidsFilters.map((filter, index) => (
          <FilterChip
            key = { 'materialization-filter-chip-' + filter.getDimensionName() + '-' + filter.getBitsFrom() + '-' + filter.getBitsTo() }
            text = { filter.getDimensionName() + ' / ' + filter.getBitsFrom() + '–' + filter.getBitsTo() }
            onDelete = { () => {
              runInAction(() => {
                store.chosenCuboidsFilters.splice(index, 1);
                store.chosenCuboidsPage = 0;
              });
              fetchChosenCuboids(store, errorStore);
            } }
          /> 
        )) }
        <AddCuboidsFilterChip onAdd = {(dimensionIndex: number, bitsFrom: number, bitsTo: number) => {
          runInAction(() => {
            store.chosenCuboidsFilters.push(buildMessage(new DimensionFilterCuboid(), {
              dimensionName: store.dimensions[dimensionIndex].name,
              bitsFrom: bitsFrom,
              bitsTo: bitsTo
            }));
            store.chosenCuboidsPage = 0;
          });
          fetchChosenCuboids(store, errorStore);
        }} />
      </Grid>
    </Grid>
    <div style = {{ marginTop: '20px' }}>
      <MaterialReactTable
        columns = { store.dimensions.map(dimensionToColumn) }
        enableColumnResizing
        data = { store.chosenCuboids.map(cuboidToRow) }
        getRowId = { row => row.id }
        enableEditing
        renderRowActions = {({row}) => (
          <Box sx={{ display: 'flex', gap: '1rem' }}>
            <Tooltip title="Delete">
              <IconButton onClick={() => {
                grpc.unary(SudokubeService.deleteChosenCuboid, {
                  host: apiBaseUrl,
                  request: buildMessage(new DeleteSelectedCuboidArgs(), {
                    cuboidIdWithinPage: row.id
                  }),
                  onEnd: response => {
                    if (response.status !== 0) {
                      runInAction(() => {
                        errorStore.errorMessage = response.statusMessage;
                        errorStore.isErrorPopupOpen = true;
                      });
                      return;
                    }
                    fetchChosenCuboids(store, errorStore);
                  }
                });
              }}>
                <Delete />
              </IconButton>
            </Tooltip>
          </Box>
        )}
        rowCount = {Number.MAX_VALUE}
        enablePagination
        manualPagination
        muiTablePaginationProps = {{
          showFirstButton: false,
          showLastButton: false
        }}
        state = {{
          density: 'compact',
          pagination: { pageIndex: store.chosenCuboidsPage, pageSize: store.chosenCuboidsPageSize }
        }}
        onPaginationChange={(updater: any) => {
          const model = updater({pageIndex: store.chosenCuboidsPage, pageSize: store.chosenCuboidsPageSize});
          runInAction(() => {
            store.chosenCuboidsPage = model.pageIndex;
            store.chosenCuboidsPageSize = model.pageSize;
          });
          fetchChosenCuboids(store, errorStore);
        }}
        muiBottomToolbarProps = {{
          sx: {
            '.MuiTablePagination-displayedRows': { display: 'none' },
            '.MuiTablePagination-selectLabel': { display: 'none' },
            '.MuiTablePagination-select': { display: 'none' },
            '.MuiTablePagination-selectIcon': { display: 'none' }
          } 
        }}
        renderTopToolbar = {false}
      />
    </div>
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

export const cuboidToRow = ((cuboid: CuboidDef, index: number) => {
  const theme = useTheme();
  let row: any = {};
  row['id'] = String(index);
  row['index'] = index;
  cuboid.getDimensionsList().forEach(dimension => 
    row[dimension.getDimensionName()] =
      dimension.getChosenBitsList().map(bit => bit ? '\u2589' : '\u25A2').join('')
  );
  return row;
});

const dimensionToColumn = ((dimension: MaterializationDimension) => ({
  accessorKey: dimension.name,
  header: dimension.name + ' (' + dimension.numBits + ' bits)'
} as MRT_ColumnDef<any>));
