import * as React from 'react';
import Container from '@mui/material/Container';
import { Box, Button, FormControl, Grid, InputLabel, MenuItem, Select, Table, TableBody, TableCell, TableContainer, TableHead, TableRow } from '@mui/material'
import { DimensionChip, AddDimensionChip, FilterChip, AddFilterChip } from './QueryViewChips';
import { ResponsiveLine } from '@nivo/line';
import { observer } from 'mobx-react-lite';
import { apiBaseUrl, useRootStore } from './RootStore';
import { ButtonChip, SelectionChip } from './GenericChips';
import { runInAction } from 'mobx';
import { DataGrid, GridColDef } from '@mui/x-data-grid';
import { useEffect } from 'react';
import { grpc } from '@improbable-eng/grpc-web';
import { DeleteFilterArgs, Empty, GetCubesResponse, GetFiltersResponse, QueryArgs, QueryResponse, SelectDataCubeArgs, SelectDataCubeForQueryResponse } from './_proto/sudokubeRPC_pb';
import { SudokubeService } from './_proto/sudokubeRPC_pb_service';
import { buildMessage } from './Utils';
import { cuboidToRow } from './MaterializationView';

const data = require('./sales-data.json');

export default observer(function Query() {
  const { queryStore: store } = useRootStore();
  useEffect(() => {
    grpc.unary(SudokubeService.getDataCubesForQuery, {
      host: apiBaseUrl,
      request: new Empty(),
      onEnd: response => {
        runInAction(() => {
          store.cubes = (response.message as GetCubesResponse)?.getCubesList();
          store.cube = store.cubes[0];
        });
        grpc.unary(SudokubeService.selectDataCubeForQuery, {
          host: apiBaseUrl,
          request: buildMessage(new SelectDataCubeArgs(), {cube: store.cube}),
          onEnd: response => runInAction(() => {
            const message = response.message as SelectDataCubeForQueryResponse;
            runInAction(() => {
              store.dimensions = message.getDimensionsList();
              store.measures = message.getMeasuresList();
            });
          })
        });
      }
    });
  }, []);
  return (
    <Container style = {{ paddingTop: '20px' }}>
      <SelectCube/>
      <QueryParams/>
      <Cuboids isShown = {store.isRunComplete}/>
      <Chart isShown = {store.isRunComplete}/>
      <Metrics isShown = {store.isRunComplete}/>
    </Container>
  )
})

const SelectCube = observer(() => {
  const { queryStore: store } = useRootStore();
  return ( <div>
    <FormControl sx = {{ minWidth: 200 }}>
      <InputLabel htmlFor = "select-cube">Select Cube</InputLabel>
      <Select
        id = "select-cube" label = "Select Cube"
        style = {{ marginBottom: 10 }}
        size = 'small'
        value = { store.cube }
        onChange = { e => {
          runInAction(() => store.cube = e.target.value);
          grpc.unary(SudokubeService.selectDataCubeForQuery, {
            host: apiBaseUrl,
            request: buildMessage(new SelectDataCubeArgs(), {cube: store.cube}),
            onEnd: response => runInAction(() => {
              const message = response.message as SelectDataCubeForQueryResponse;
              runInAction(() => {
                store.dimensions = message.getDimensionsList();
                store.measures = message.getMeasuresList();
              });
            })
          });
        } }>
        { store.cubes.map(cube => (
          <MenuItem key = { 'select-cube-' + cube } value = {cube}>{cube}</MenuItem>
        )) }
      </Select>
    </FormControl>
  </div> );
})

const QueryParams = observer(() => {
  const { queryStore: store } = useRootStore();
  return (
    <Grid container maxHeight='30vh' overflow='scroll' style={{ paddingTop: '1px', paddingBottom: '1px' }}>
      <Horizontal/>
      <Filters/>
      <Series/>
      <Grid item xs={6}>
        <SelectionChip 
          keyText = 'Measure' 
          valueText = { store.measure } 
          valueRange = { store.measures } 
          onChange = { v => runInAction(() => store.measure = v) }
        />
        <SelectionChip 
          keyText = 'Aggregation' 
          valueText = { store.aggregation } 
          valueRange = { store.aggregations } 
          onChange = { v => runInAction(() => store.aggregation = v) }
        />
        <SelectionChip 
          keyText = 'Solver' 
          valueText = { store.solver } 
          valueRange = { store.solvers } 
          onChange = { v => runInAction(() => store.solver = v) }
        />
        <SelectionChip
          keyText = 'Mode'
          valueText = { store.mode }
          valueRange = { store.modes }
          onChange = { v => runInAction(() => store.mode = v) }
        />
        <ButtonChip label = 'Run' variant = 'filled' onClick = {() => {
          grpc.unary(SudokubeService.startQuery, {
            host: apiBaseUrl,
            request: buildMessage(new QueryArgs(), {
              horizontalList: store.horizontal.map(dimension => buildMessage(new QueryArgs.DimensionDef(), {
                dimensionName: store.dimensions[dimension.dimensionIndex],
                dimensionLevel: store.dimensions[dimension.dimensionIndex].getLevelsList()[dimension.dimensionLevelIndex]
              })),
              seriesList: store.series.map(dimension => buildMessage(new QueryArgs.DimensionDef(), {
                dimensionName: store.dimensions[dimension.dimensionIndex],
                dimensionLevel: store.dimensions[dimension.dimensionIndex].getLevelsList()[dimension.dimensionLevelIndex]
              })),
              measure: store.measure,
              aggregation: store.aggregation,
              solver: store.solver,
              isBatchMode: store.mode === 'Batch',
              preparedCuboidsPerPage: store.cuboidsPageSize
            }),
            onEnd: res => {
              const message = res.message as QueryResponse;
              runInAction(() => {
                store.cuboidsPage = message.getCuboidsPageId();
                store.preparedCuboids = message.getCuboidsList();
                store.currentCuboidIdWithinPage = message.getCurrentCuboidIdWithinPage();
                store.isQueryComplete = message.getIsComplete();
                store.result = { data: message.getSeriesList().map(seriesData => ({
                  id: seriesData.getSeriesName(),
                  data: seriesData.getDataList().map(point => point.toObject())
                })) };
                store.metrics = message.getStatsList().map(stat => stat.toObject());
                store.isRunComplete = true;
              })
            }
          })
        }} />
      </Grid>
    </Grid>
  )
})

const Horizontal = observer(() => {
  const { queryStore: store } = useRootStore();
  return (
    <Grid item xs={6}>
      { store.horizontal.map((d, i) => (<DimensionChip
        key = {'horizontal-' + d.dimensionIndex + '-' + d.dimensionLevelIndex}
        type = 'Horizontal'
        text = {
          store.dimensions[d.dimensionIndex].getDimName() 
            + ' / ' 
            + store.dimensions[d.dimensionIndex].getLevelsList()[d.dimensionLevelIndex]
        }
        zoomIn = { () => store.zoomInHorizontal(i) }
        zoomOut = { () => store.zoomOutHorizontal(i) }
        onDelete = { () => runInAction(() => store.horizontal.splice(i, 1)) }
      />)) }
      <AddDimensionChip type='Horizontal' />
    </Grid>
  )
});

const Filters = observer(() => {
  const { queryStore: store } = useRootStore();
  return (
    <Grid item xs={6}>
      { store.filters.map((d, i) => (<FilterChip
        key = {'filter-' + d.getDimensionName() + '-' + d.getDimensionLevel() + '-' + d.getValues()}
        text = { d.getDimensionName() + ' / ' + d.getDimensionLevel() + ' = ' + d.getValues() }
        onDelete = { () => {
          grpc.unary(SudokubeService.deleteFilter, {
            host: apiBaseUrl,
            request: buildMessage(new DeleteFilterArgs(), { index: i }),
            onEnd: () => {
              grpc.unary(SudokubeService.getFilters, {
                host: apiBaseUrl,
                request: new Empty(),
                onEnd: (res => runInAction(() => store.filters = (res.message as GetFiltersResponse).getFiltersList()))
              });
            }
          })
        } }
      />)) }
      <AddFilterChip/>
    </Grid>
  )
});

const Series = observer(() => {
  const { queryStore } = useRootStore();
  const dimensions = queryStore.dimensions;
  return (
    <Grid item xs={6}>
      { queryStore.series.map((d, i) => (
        <DimensionChip
          key = {'series-' + d.dimensionIndex + '-' + d.dimensionLevelIndex}
          type = 'Series'
          text = {
            dimensions[d.dimensionIndex].getDimName()
              + ' / ' 
              + dimensions[d.dimensionIndex].getLevelsList()[d.dimensionLevelIndex]
          }
          zoomIn = { () => queryStore.zoomInSeries(i) }
          zoomOut = { () => queryStore.zoomOutSeries(i) }
          onDelete = { () => runInAction(() => queryStore.series.splice(i, 1)) }
        />
      )) }
      <AddDimensionChip type='Series' />
    </Grid>
  );
});

const Cuboids = observer(({isShown}: {isShown: boolean}) => {
  const { queryStore: store } = useRootStore();
  if (!isShown || store.preparedCuboids.length === 0) {
    return null;
  }
  return ( <div>
    <h3>Prepared Cuboids</h3>
    <Box sx = {{ height: '30vh', width: '100%', marginTop: '20px' }}>
      <DataGrid
        rows = { store.preparedCuboids.map(cuboidToRow) }
        columns = { store.dimensions.map(dimensionToColumn) }
        disableRowSelectionOnClick
        sx = {{
          overflowX: 'scroll',
          '.materialization-online-next-cuboid': {
            color: 'primary.main'
          },
          // https://github.com/mui/mui-x/issues/409#issuecomment-1233333917
          '.MuiTablePagination-displayedRows': {
            display: 'none',
          },
        }}
        density = 'compact'
        getRowClassName = {(params) =>
          params.row.index === store.nextCuboidIndex ? 'materialization-online-next-cuboid' : ''
        }
        initialState = {{
          pagination: {
            paginationModel: { pageSize: 5, page: 2 }
          }
        }}
        pagination = {true}
        paginationMode="server"
        // paginationModel={{pageSize: 5, page: 2}}
        pageSizeOptions={[5]}
        rowCount={Number.MAX_VALUE}
        onPaginationModelChange={(model) => {console.log(model.page)}}
        // rowCount={10}
      />
    </Box>
    <Button
      disabled = {store.isQueryComplete}
      onClick = {() => {
        grpc.unary(SudokubeService.continueQuery, {
          host: apiBaseUrl,
          request: new Empty(),
          onEnd: res => {
            const message = res.message as QueryResponse;
            runInAction(() => {
              store.cuboidsPage = message.getCuboidsPageId();
              store.preparedCuboids = message.getCuboidsList();
              store.currentCuboidIdWithinPage = message.getCurrentCuboidIdWithinPage();
              store.isQueryComplete = message.getIsComplete();
              store.result = { data: message.getSeriesList().map(seriesData => ({
                id: seriesData.getSeriesName(),
                data: seriesData.getDataList().map(point => point.toObject())
              })) };
              store.metrics = message.getStatsList().map(stat => stat.toObject());
              store.isRunComplete = true;
            })
          }
        })
      }}
    >Continue</Button>
  </div> );
})

const Chart = observer(({isShown}: {isShown: boolean}) => {
  if (!isShown) {
    return null;
  }
  const { queryStore: store } = useRootStore();
  return ( <div>
    <h3>Current result</h3>
    <div style={{ width: '100%', height: '50vh' }}>
      <ResponsiveLine
        data = { store.result.data }
        margin={{ top: 5, right: 115, bottom: 25, left: 35 }}
        yScale={{
          type: 'linear',
          min: 'auto',
          max: 'auto',
          stacked: false,
          reverse: false
        }}
        yFormat=' >-.2f'
        axisTop={null}
        axisRight={null}
        pointLabelYOffset={-12}
        useMesh={true}
        legends={[
          {
            anchor: 'bottom-right',
            direction: 'column',
            justify: false,
            translateX: 100,
            translateY: 0,
            itemsSpacing: 0,
            itemDirection: 'left-to-right',
            itemWidth: 80,
            itemHeight: 20,
            itemOpacity: 0.75,
            symbolSize: 12,
            symbolShape: 'circle',
            symbolBorderColor: 'rgba(0, 0, 0, .5)',
            effects: [
              {
                on: 'hover',
                style: {
                  itemBackground: 'rgba(0, 0, 0, .03)',
                  itemOpacity: 1
                }
              }
            ]
          }
        ]}
      />
    </div>
  </div> );
})

const Metrics = observer(({isShown}: {isShown: boolean}) => {
  if (!isShown) {
    return null;
  }
  const { queryStore: store } = useRootStore();
  return ( <div>
    <h3>Metrics</h3>
    <TableContainer>
      <Table sx = {{ width: 'auto' }} aria-label = "simple table" size = 'small'>
        <TableHead>
          <TableRow>
            { store.metrics.map(metric => <TableCell>{metric.name}</TableCell>) }
          </TableRow>
        </TableHead>
        <TableBody>
          { store.metrics.map(metric => <TableCell>{metric.value}</TableCell>) }
        </TableBody>
      </Table>
    </TableContainer>
  </div> );
});

const dimensionToColumn = ((dimension: SelectDataCubeForQueryResponse.DimHierarchy) => ({
  field: dimension.getDimName(),
  type: 'string',
  headerName: dimension.getDimName() + ' (' + dimension.getNumBits() + ' bits)',
  sortable: false,
  disableColumnMenu: true,
  width: dimension.getNumBits() * 20 + 20
} as GridColDef));
