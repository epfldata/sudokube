import React, { ReactNode, useCallback, useEffect, useState } from 'react'
import { Box, Button, Chip, Dialog, DialogActions, DialogContent, DialogTitle, FormControl, InputLabel, MenuItem, Select, TextField } from '@mui/material'
import FilterAltIcon from '@mui/icons-material/FilterAlt';
import ArrowForwardIcon from '@mui/icons-material/ArrowForward';
import SsidChartIcon from '@mui/icons-material/SsidChart';
import AddIcon from '@mui/icons-material/Add';
import RemoveIcon from '@mui/icons-material/Remove';
import { apiBaseUrl, useRootStore } from './RootStore';
import { chipStyle, InChipButton } from './GenericChips';
import { observer } from 'mobx-react-lite';
import { SudokubeService } from './_proto/sudokubeRPC_pb_service';
import { buildMessage } from './Utils';
import { Empty, GetFiltersResponse, GetSliceValueResponse, GetSliceValuesArgs, SetSliceValuesArgs } from './_proto/sudokubeRPC_pb';
import { grpc } from '@improbable-eng/grpc-web';
import { runInAction } from 'mobx';
import MaterialReactTable, { MRT_RowSelectionState } from 'material-react-table';

export function DimensionChip({ type, text, zoomIn, zoomOut, onDelete }: {
  type: 'Horizontal' | 'Series', 
  text: ReactNode, 
  zoomIn: () => void,
  zoomOut: () => void
  onDelete: (event: any) => void
}) {
  return (<Chip
    style = {chipStyle}
    variant = 'outlined'
    icon = { 
      type == 'Horizontal' 
        ? <ArrowForwardIcon style = {{ height: '15px' }} /> 
        : <SsidChartIcon style = {{ height: '15px' }} />
    }
    label = { <span>
      {text} 
      <InChipButton
        icon = { <AddIcon style = {{ width: '16px', height: '16px', marginTop: '3px' }} /> }
        onClick = {zoomIn} 
      />
      <InChipButton 
        icon = { <RemoveIcon style = {{ width: '16px', height: '16px', marginTop: '3px' }} /> } 
        onClick = {zoomOut}
      />
    </span> }
    onDelete = {onDelete}
  />)
}

export const AddDimensionChip = observer(({ type }: { type: 'Horizontal' | 'Series'}) => {
  const { queryStore: store } = useRootStore();
  const [isDialogOpen, setDialogOpen] = React.useState(false);
  const [dimensionIndex, setDimensionIndex] = React.useState(0);
  const [dimensionLevelIndex, setDimensionLevelIndex] = React.useState(0);

  return (<span>
    <Chip
      style={chipStyle}
      variant='outlined'
      icon = { 
        type == 'Horizontal' 
          ? <ArrowForwardIcon style = {{ height: '15px' }} /> 
          : <SsidChartIcon style = {{ height: '15px' }} />
      }
      label = 'Add ...'
      color = 'primary'
      onClick = {() => { setDialogOpen(true) }}
      disabled = {!store.isCubeLoaded}
    />

    <Dialog open = {isDialogOpen}>
      <DialogTitle>Add Dimension</DialogTitle>
      <DialogContent>
        <FormControl sx={{ m: 1, minWidth: 120 }}>
          <InputLabel htmlFor="add-dimension-select-dimension">Dimension</InputLabel>
          <Select
            value = {dimensionIndex}
            onChange = { e => setDimensionIndex(e.target.value as number) }
            id="add-dimension-select-dimension" label="Dimension">
            { store.dimensionHierarchy.map((dimension, index) => (
              <MenuItem key = { 'add-dimension-select-dimension-' + index } value = {index}> {dimension.getDimName()} </MenuItem>
            )) }
          </Select>
        </FormControl>
        <FormControl sx={{ m: 1, minWidth: 120 }}>
          <InputLabel htmlFor="add-dimension-select-dimension-level">Dimension Level</InputLabel>
          <Select
            value = {dimensionLevelIndex}
            onChange = { e => setDimensionLevelIndex(e.target.value as number) }
            id="add-dimension-select-dimension-level" label="Dimension Level">
            { store.dimensionHierarchy[dimensionIndex]?.getLevelsList().map((level, index) => (
              <MenuItem key = { 'add-dimension-select-dimension-level-' + index } value = {index}>{level}</MenuItem>
            )) }
          </Select>
        </FormControl>
        <DialogActions>
          <Button onClick = { () => setDialogOpen(false) }>Cancel</Button>
          <Button onClick = { () => { 
            switch (type) {
              case 'Horizontal': store.addHorizontal(dimensionIndex, dimensionLevelIndex); break;
              case 'Series': store.addSeries(dimensionIndex, dimensionLevelIndex); break;
            }
            setDialogOpen(false);
          } }>Add</Button>
        </DialogActions>
      </DialogContent>
    </Dialog>
  </span>)
});

export function FilterChip({ text, onDelete }: {
  text: ReactNode, onDelete: ((event: any) => void) | undefined 
}) {
  return (<Chip
    style = {chipStyle}
    variant = 'outlined'
    icon = { <FilterAltIcon style = {{ height: '18px' }} /> }
    label = {text} 
    onDelete = {onDelete}
  />)
}

export const AddFilterChip = observer(() => {
  const { queryStore: store, errorStore } = useRootStore();
  const [isDialogOpen, setDialogOpen] = React.useState(false);
  const [dimensionIndex, setDimensionIndex] = useState(0);
  const [level, setLevel] = useState('');
  const [valuesSearchText, setValuesSearchText] = useState('');
  const [values, setValues] = useState<string[]>([]);
  const [rowSelectionModel, setRowSelectionModel] = useState<MRT_RowSelectionState>({});
  const [page, setPage] = useState(0);
  const [pageSize, setPageSize] = useState(10);

  useEffect(() => {
    if (isDialogOpen) {
      setDimensionIndex(0);
      setLevel('');
      setValuesSearchText('');
      setValues([]);
      setRowSelectionModel({});
      setPage(0);
      setPageSize(10);
    }
  }, [isDialogOpen]);

  const fetchValues = useCallback(({newDimension, newLevel, newSearchText, newPageId, newPageSize}: {
    newDimension?: string, newLevel?: string, newSearchText?: string, newPageId?: number, newPageSize?: number
  }) => {
    grpc.unary(SudokubeService.getValuesForSlice, {
      host: apiBaseUrl,
      request: buildMessage(new GetSliceValuesArgs(), {
        dimensionName: newDimension ?? store.dimensionHierarchy[dimensionIndex].getDimName(),
        dimensionLevel: newLevel ?? level,
        searchText: newSearchText ?? valuesSearchText,
        requestedPageId: newPageId ?? page,
        numRowsInPage: newPageSize ?? pageSize
      }),
      onEnd: (res => {
        if (res.status !== 0) {
          runInAction(() => {
            errorStore.errorMessage = res.statusMessage;
            errorStore.isErrorPopupOpen = true;
          });
          return;
        }
        const message = res.message as GetSliceValueResponse;
        setValues(message.getValuesList());
        const newModel: MRT_RowSelectionState = {};
        message.getIsSelectedList().forEach((isSelected, index) => newModel[String(index)] = isSelected)
        setRowSelectionModel(newModel);
      })
    });
  }, [store.dimensionHierarchy, dimensionIndex, level, valuesSearchText, page, pageSize]);

  const fetchFilters = useCallback(() => {
    grpc.unary(SudokubeService.getFilters, {
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
        runInAction(() => store.filters = (response.message as GetFiltersResponse).getFiltersList());
      }
    })
  }, []);

  return (<span>
    <Chip
      icon = { <FilterAltIcon style = {{ height: '18px' }} /> }
      label = 'Add ...'
      onClick = { () => setDialogOpen(true) }
      disabled = {!store.isCubeLoaded}
      style = {chipStyle}
      variant = 'outlined'
      color = 'primary'
    />
    <Dialog open = {isDialogOpen}>
      <DialogTitle>Add Filter</DialogTitle>
      <DialogContent>
        <div>
          <FormControl sx={{ m: 1, minWidth: 200 }}>
            <InputLabel htmlFor="add-query-filter-select-dimension">Dimension</InputLabel>
            <Select
              value = {dimensionIndex}
              onChange = { e => setDimensionIndex(e.target.value as number) }
              id="add-query-filter-select-dimension" label="Dimension">
              { store.dimensionHierarchy.map((dimension, index) => (
                <MenuItem key = { 'add-query-filter-select-dimension-' + index } value = {index}> {dimension.getDimName()} </MenuItem>
              )) }
            </Select>
          </FormControl>
          <FormControl sx={{ m: 1, minWidth: 200 }}>
            <InputLabel htmlFor="add-query-filter-select-dimension-level">Dimension Level</InputLabel>
            <Select
              value = {level}
              onChange = { e => {
                setLevel(e.target.value);
                setValuesSearchText('');
                setPage(0);
                fetchValues({newLevel: e.target.value, newSearchText: '', newPageId: 0});
              }}
              id="add-query-filter-select-dimension-level" label="Dimension Level">
              { store.dimensionHierarchy[dimensionIndex]?.getLevelsList().map((dimensionLevel, index) => (
                <MenuItem key = { 'add-query-filter-select-dimension-' + dimensionIndex + '-level-' + index } value = {dimensionLevel}>{dimensionLevel}</MenuItem>
              )) }
            </Select>
          </FormControl>
        </div>
        <div>
          <TextField
            value = {valuesSearchText}
            onChange = { e => {
              setValuesSearchText(e.target.value);
              fetchValues({newSearchText: e.target.value});
            } }
            id="add-query-filter-search-text" label="Search for values"/>
        </div>
        <div>
          <Box sx = {{ marginTop: '20px' }}>
            <MaterialReactTable
              columns = { [{accessorKey: 'Value', header: 'Value'}] }
              enableColumnResizing
              data = { values.map((value, index) => ({ id: String(index), 'Value': value })) } 
              getRowId = { row => row.id }
              enableRowSelection
              rowCount = {Number.MAX_VALUE}
              enablePagination
              manualPagination
              state = {{
                density: 'compact',
                pagination: { pageIndex: page, pageSize: pageSize },
                rowSelection: rowSelectionModel
              }}
              onRowSelectionChange={setRowSelectionModel}
              onPaginationChange={(updater: any) => {
                const model = updater({pageIndex: page, pageSize: pageSize});
                setPage(model.pageIndex);
                setPageSize(model.pageSize);
                fetchValues({newPageId: model.pageIndex, newPageSize: model.pageSize});
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
          </Box>
        </div>
        <DialogActions>
          <Button onClick = { () => {
            grpc.unary(SudokubeService.setValuesForSlice, {
              host: apiBaseUrl,
              request: buildMessage(new SetSliceValuesArgs(), {
                isSelectedList: Array.from(values.keys()).map(i => rowSelectionModel[String(i)])
              }),
              onEnd: response => {
                if (response.status !== 0) {
                  runInAction(() => {
                    errorStore.errorMessage = response.statusMessage;
                    errorStore.isErrorPopupOpen = true;
                  });
                  return;
                }
                fetchFilters();
                setDialogOpen(false);
              }
            });
          } }>Done</Button>
        </DialogActions>
      </DialogContent>
    </Dialog>
  </span>)
});

export const MeasuresChip = observer(({ measure1, measure2, measures, onChange1, onChange2 }: {
  measure1: string, measure2?: string, measures: string[],
  onChange1: (value: string) => void, onChange2?: (value: string) => void
}) => {
  const { queryStore: store } = useRootStore();
  const [isDialogOpen, setDialogOpen] = React.useState(false);
  return (<span>
    <Chip 
      style = {chipStyle}
      variant = 'outlined'
      label = { <span><b>Measures </b>{measure1 + (measure2 ? ', ' + measure2 : '')}</span> }
      onClick = { () => { setDialogOpen(true) } } 
      disabled = {!store.isCubeLoaded}
    />
    <Dialog open = {isDialogOpen}>
      <DialogTitle> { "Select Measures" } </DialogTitle>
      <DialogContent>
        <FormControl sx = {{ m: 1, minWidth: 120 }}>
          <InputLabel htmlFor = { "select-measure-1" }>{'Measure' + (measure2 ? ' 1' : '')}</InputLabel>
          <Select
            value = {measure1}
            onChange = { e => {
              measure1 = e.target.value;
              onChange1(measure1);
            } }
            id = { 'select-measure-1' } label = {'Measure' + (measure2 ? ' 1' : '')}>
            { measures.map(value => (
              <MenuItem key = { 'select-measure-1-' + value } value = {value}> {value} </MenuItem>
            )) }
          </Select>
        </FormControl>
        { measure2 !== undefined ? (
          <FormControl sx = {{ m: 1, minWidth: 120 }}>
            <InputLabel htmlFor = { 'select-measure-2' }>Measure 2</InputLabel>
            <Select
              value = {measure2}
              onChange = { e => {
                measure2 = e.target.value;
                onChange2!(measure2!);
              } }
              id = { 'select-measure-2' } label = 'Measure 2'>
              { measures.map(value => (
                <MenuItem key = { 'select-measure-2-' + value } value = {value}> {value} </MenuItem>
              )) }
            </Select>
          </FormControl>
        ) : null }
        <DialogActions>
          <Button onClick = { () => setDialogOpen(false) }>Done</Button>
        </DialogActions>
      </DialogContent>
    </Dialog>
  </span>)
});
