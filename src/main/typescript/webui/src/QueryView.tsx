import * as React from 'react';
import Container from '@mui/material/Container';
import { Box, Button, FormControl, Grid, InputLabel, MenuItem, Select, Table, TableBody, TableCell, TableContainer, TableHead, TableRow } from '@mui/material'
import { DimensionChip, AddDimensionChip, FilterChip, AddQueryFilterChip } from './QueryViewChips';
import { ResponsiveLine } from '@nivo/line';
import { observer } from 'mobx-react-lite';
import { useRootStore } from './RootStore';
import { ButtonChip, SelectionChip } from './GenericChips';
import { runInAction } from 'mobx';
import { DataGrid } from '@mui/x-data-grid';
import { dimensionToColumn } from './MaterializationView';
import { Cuboid } from './MaterializationStore';

const data = require('./sales-data.json');

export default observer(function Query() {
  const { queryStore: store } = useRootStore();
  // TODO: Call backend to load available cubes
  runInAction(() => store.cubes = ['sales']);
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
        value = { store.selectedCubeIndex }
        onChange = { e => {
          runInAction(() => store.selectedCubeIndex = e.target.value as number);
          // TODO: Load dimension hierarchy and flat dimensions
        } }>
        { store.cubes.map((cube, index) => (
          <MenuItem key = { 'select-cube-' + index } value = {index}>{cube}</MenuItem>
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
          // TODO: Call backend to query
          runInAction(() => {
            store.isRunComplete = true;
            store.result = data;
            store.preparedCuboids = [{
              id: 0,
              dimensions: [
                { name: "Country", bits: '\u2589\u2589\u2589\u25A2\u25A2\u25A2' },
                { name: "City", bits: '\u25A2\u25A2\u25A2\u25A2\u25A2\u25A2' },
                { name: "Year", bits: '\u25A2\u25A2\u25A2\u25A2\u25A2\u25A2' },
                { name: "Month", bits: '\u2589\u2589\u2589\u2589\u25A2' },
                { name: "Day", bits: '\u25A2\u25A2\u25A2\u25A2\u25A2\u25A2'}
              ]
            }];
            store.metrics = [
              { name: 'Prepare time', value: '1 s' },
              { name: 'Fetch time', value: '1 s' },
              { name: 'Solve time', value: '1 s' },
              { name: 'Error', value: '0.01' },
              { name: 'Degree of freedom', value: '?' }
            ];
          });
        }} />
      </Grid>
    </Grid>
  )
})

const Horizontal = observer(() => {
  const { queryStore: store } = useRootStore();
  const dimensions = store.cube.dimensionHierarchy.dimensions;
  return (
    <Grid item xs={6}>
      { store.horizontal.map((d, i) => (<DimensionChip
        key = {'horizontal-' + d.dimensionIndex + '-' + d.dimensionLevelIndex}
        type = 'Horizontal'
        text = {
          dimensions[d.dimensionIndex].name 
            + ' / ' 
            + dimensions[d.dimensionIndex].dimensionLevels[d.dimensionLevelIndex].name
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
  const dimensions = store.cube.dimensionHierarchy.dimensions;
  return (
    <Grid item xs={6}>
      { store.filters.map((d, i) => (<FilterChip
        key = {'filter-' + d.dimensionIndex + '-' + d.dimensionLevelIndex + '-' + d.valueIndex}
        text = {
          dimensions[d.dimensionIndex].name 
            + ' / ' 
            + dimensions[d.dimensionIndex].dimensionLevels[d.dimensionLevelIndex].name
            + ' = '
            + dimensions[d.dimensionIndex].dimensionLevels[d.dimensionLevelIndex].possibleValues[d.valueIndex]
        }
        onDelete = { () => runInAction(() => store.filters.splice(i, 1)) }
      />)) }
      <AddQueryFilterChip/>
    </Grid>
  )
});

const Series = observer(() => {
  const { queryStore } = useRootStore();
  const dimensions = queryStore.cube.dimensionHierarchy.dimensions;
  return (
    <Grid item xs={6}>
      { queryStore.series.map((d, i) => (
        <DimensionChip
          key = {'series-' + d.dimensionIndex + '-' + d.dimensionLevelIndex}
          type = 'Series'
          text = {
            dimensions[d.dimensionIndex].name 
              + ' / ' 
              + dimensions[d.dimensionIndex].dimensionLevels[d.dimensionLevelIndex].name
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
        columns = { store.cube.dimensions.map(dimensionToColumn) }
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
        // data = { store.result.data }
        data = {[
          {id: 'Stuff', data: [{x: 1, y: 1}, {x: 3, y: 3}]}
        ]}
        margin={{ top: 5, right: 115, bottom: 25, left: 35 }}
        xScale={{ type: 'linear' }}
        xFormat = {(v) => v === 1.0 ? 'one' : 'others'}
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
        axisBottom={{
          tickValues: [1, 3],
          format: (v) => (v === 1 ? 'one' : 'three')
        }}
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

const cuboidToRow = ((cuboid: Cuboid, index: number) => {
  let row: any = {};
  row["id"] = cuboid.id;
  row['index'] = index;
  cuboid.dimensions.forEach(dimension => row[dimension.name] = dimension.bits);
  return row;
});