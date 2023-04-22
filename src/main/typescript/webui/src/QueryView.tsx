import * as React from 'react';
import Container from '@mui/material/Container';
import { FormControl, Grid, InputLabel, MenuItem, Select } from '@mui/material'
import { DimensionChip, AddDimensionChip, FilterChip, AddQueryFilterChip } from './QueryViewChips';
import { ResponsiveLine } from '@nivo/line';
import { observer } from 'mobx-react-lite';
import { useRootStore } from './RootStore';
import { ButtonChip, SelectionChip } from './GenericChips';
import { runInAction } from 'mobx';

const data = require('./sales-data.json');

export default observer(function Query() {
  const { queryStore } = useRootStore();
  // TODO: Call backend to load available cubes
  runInAction(() => queryStore.cubes = ['sales']);
  return (
    <Container style = {{ paddingTop: '20px' }}>
      <SelectCube/>
      <QueryParams/>
      <Chart/>
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
          // TODO: Load dimension hierarchy
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
        <ButtonChip label = 'Run' variant = 'filled' onClick = {() => {
          // TODO: Call backend to query
          runInAction(() => store.result = data);
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

const Chart = observer(() => {
  const { queryStore: store } = useRootStore();
  return (
    <div style={{ width: '100%', height: '80vh', paddingTop: '20px' }}>
      <ResponsiveLine
        data = { store.result.data }
        margin={{ top: 5, right: 115, bottom: 25, left: 35 }}
        xScale={{ type: 'point' }}
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
  )
})
