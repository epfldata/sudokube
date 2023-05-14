import React, { ReactNode, useCallback, useState } from 'react'
import { Box, Button, Chip, Dialog, DialogActions, DialogContent, DialogTitle, FormControl, FormLabel, Grid, Input, InputLabel, MenuItem, Select, Table, TextField } from '@mui/material'
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
import { DataGrid, GridRowSelectionModel } from '@mui/x-data-grid';
import { runInAction } from 'mobx';

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

export function AddDimensionChip({ type }: { type: 'Horizontal' | 'Series'}) {
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
            { store.dimensions.map((dimension, index) => (
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
            { store.dimensions[dimensionIndex]?.getLevelsList().map((level, index) => (
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
}

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
  const { queryStore: store } = useRootStore();
  const [isDialogOpen, setDialogOpen] = React.useState(false);
  const [dimensionIndex, setDimensionIndex] = useState(0);
  const [level, setLevel] = useState('');
  const [valuesSearchText, setValuesSearchText] = useState('');
  const [values, setValues] = useState<string[]>([]);
  const [rowSelectionModel, setRowSelectionModel] = useState<GridRowSelectionModel>([]);
  const [page, setPage] = useState(0);
  const [pageSize, setPageSize] = useState(10);

  const fetchValues = useCallback(({newDimension, newLevel, newSearchText, newPageId, newPageSize}: {
    newDimension?: string, newLevel?: string, newSearchText?: string, newPageId?: number, newPageSize?: number
  }) => {
    grpc.unary(SudokubeService.getValuesForSlice, {
      host: apiBaseUrl,
      request: buildMessage(new GetSliceValuesArgs(), {
        dimensionName: newDimension ?? store.dimensions[dimensionIndex].getDimName(),
        dimensionLevel: newLevel ?? level,
        searchText: newSearchText ?? valuesSearchText,
        requestedPageId: newPageId ?? page,
        numRowsInPage: newPageSize ?? pageSize
      }),
      onEnd: (res => {
        const message = res.message as GetSliceValueResponse;
        setValues(message.getValuesList());
        setRowSelectionModel(values.flatMap((_, index) => message.getIsSelectedList()[index] ? [index] : []));
      })
    });
  }, [store.dimensions, dimensionIndex, level, valuesSearchText, page, pageSize]);

  const fetchFilters = useCallback(() => {
    grpc.unary(SudokubeService.getFilters, {
      host: apiBaseUrl,
      request: new Empty(),
      onEnd: (res => runInAction(() => store.filters = (res.message as GetFiltersResponse).getFiltersList()))
    })
  }, []);

  return (<span>
    <Chip
      icon = { <FilterAltIcon style = {{ height: '18px' }} /> }
      label = 'Add ...'
      onClick = { () => setDialogOpen(true) }
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
              { store.dimensions.map((dimension, index) => (
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
                fetchValues({});
              }}
              id="add-query-filter-select-dimension-level" label="Dimension Level">
              { store.dimensions[dimensionIndex]?.getLevelsList().map((dimensionLevel, index) => (
                <MenuItem key = { 'add-query-filter-select-dimension-' + dimensionIndex + '-level-' + index } value = {index}> {dimensionLevel} </MenuItem>
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
          <Box sx = {{ height: '60vh', width: '100%', marginTop: '20px' }}>
            <DataGrid
              rows = { values.map((value, index) => ({ id: index, 'Value': value })) } 
              columns = { [{field: 'Value'}] }
              checkboxSelection
              rowSelectionModel={rowSelectionModel}
              onRowSelectionModelChange={(model: GridRowSelectionModel) => {
                setRowSelectionModel(model);
              }}
              sx = {{
                width: '100%',
                overflowX: 'scroll',
                '.MuiTablePagination-displayedRows': {
                  display: 'none',
                }
              }} 
              density = 'compact'
              pagination = {true}
              paginationMode="server"
              paginationModel={{page: page, pageSize: pageSize}}
              pageSizeOptions={[10]}
              rowCount={Number.MAX_VALUE}
              onPaginationModelChange={model => {
                grpc.unary(SudokubeService.setValuesForSlice, {
                  host: apiBaseUrl,
                  request: buildMessage(new SetSliceValuesArgs(), {
                    isSelectedList: Array.from(Array(pageSize).keys()).map(i => rowSelectionModel.includes(i))
                  }),
                  onEnd: () => {
                    setPage(model.page);
                    setPageSize(model.pageSize);
                    fetchValues({newPageId: model.page, newPageSize: model.pageSize});
                  }
                });
              }}
            />
          </Box>
        </div>
        <DialogActions>
          <Button onClick = { () => {
            grpc.unary(SudokubeService.setValuesForSlice, {
              host: apiBaseUrl,
              request: buildMessage(new SetSliceValuesArgs(), {
                isSelectedList: Array.from(Array(pageSize).keys()).map(i => rowSelectionModel.includes(i))
              }),
              onEnd: () => {
                setDialogOpen(false);
              }
            });
          } }>Done</Button>
        </DialogActions>
      </DialogContent>
    </Dialog>
  </span>)
})