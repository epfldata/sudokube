import * as React from 'react';
import Container from '@mui/material/Container';
import { FormControl, Grid, InputLabel, MenuItem, Select } from '@mui/material'
import { DimensionChip, AddDimensionChip, FilterChip, AddQueryFilterChip } from './QueryViewChips';
import { ResponsiveLine } from '@nivo/line';
import { observer } from 'mobx-react-lite';
import { useRootStore } from './RootStore';
import { ButtonChip, SelectionChip } from './GenericChips';

const data = require('./sales-data.json');

export default observer(function Query() {
  const { queryStore } = useRootStore();
  // TODO: Call backend to load available cubes
  queryStore.setCubes(['sales']);
  return (
    <Container style = {{ paddingTop: '20px' }}>
      <SelectCube/>
      <QueryParams/>
      <Chart/>
    </Container>
  )
})

const SelectCube = observer(() => {
  const { queryStore } = useRootStore();
  return ( <div>
    <FormControl sx = {{ minWidth: 200 }}>
      <InputLabel htmlFor = "select-cube">Select Cube</InputLabel>
      <Select
        id = "select-cube" label = "Select Cube"
        style = {{ marginBottom: 10 }}
        size = 'small'
        value = { queryStore.selectedCubeIndex }
        onChange = { e => {
          queryStore.setCubeIndex(e.target.value as number);
          // TODO: Load dimension hierarchy
        } }>
        { queryStore.cubes.map((cube, index) => (
          <MenuItem key = { 'select-cube-' + index } value = {index}>{cube}</MenuItem>
        )) }
      </Select>
    </FormControl>
  </div> );
})

const QueryParams = observer(() => {
  const { queryStore } = useRootStore();
  return (
    <Grid container maxHeight='30vh' overflow='scroll' style={{ paddingTop: '1px', paddingBottom: '1px' }}>
      <Horizontal/>
      <Filters/>
      <Series/>
      <Grid item xs={6}>
        <SelectionChip 
          keyText = 'Measure' 
          valueText = { queryStore.measure } 
          valueRange = { queryStore.measures } 
          onChange = { v => queryStore.setMeasure(v) }
        />
        <SelectionChip 
          keyText = 'Aggregation' 
          valueText = { queryStore.aggregation } 
          valueRange = { queryStore.aggregations } 
          onChange = { v => queryStore.setAggregation(v) }
        />
        <SelectionChip 
          keyText = 'Solver' 
          valueText = { queryStore.solver } 
          valueRange = { queryStore.solvers } 
          onChange = { v => queryStore.setSolver(v) }
        />
        <ButtonChip label = 'Run' variant = 'filled' onClick = {() => {
          // TODO: Call backend to query
          queryStore.setResult(data);
        }} />
      </Grid>
    </Grid>
  )
})

const Horizontal = observer(() => {
  const { queryStore } = useRootStore();
  const dimensions = queryStore.cube.dimensionHierarchy.dimensions;
  return (
    <Grid item xs={6}>
      { queryStore.horizontal.map((o, i) => (<DimensionChip
        key = {'horizontal-' + o.dimensionIndex + '-' + o.dimensionLevelIndex}
        type = 'Horizontal'
        text = {
          dimensions[o.dimensionIndex].name 
            + ' / ' 
            + dimensions[o.dimensionIndex].dimensionLevels[o.dimensionLevelIndex].name
        }
        zoomIn = { () => queryStore.zoomInHorizontal(i) }
        zoomOut = { () => queryStore.zoomOutHorizontal(i) }
        onDelete = { () => queryStore.removeHorizontal(i) }
      />)) }
      <AddDimensionChip type='Horizontal' />
    </Grid>
  )
});

const Filters = observer(() => {
  const { queryStore } = useRootStore();
  const dimensions = queryStore.cube.dimensionHierarchy.dimensions;
  return (
    <Grid item xs={6}>
      { queryStore.filters.map((o, i) => (<FilterChip
        key = {'filter-' + o.dimensionIndex + '-' + o.dimensionLevelIndex + '-' + o.valueIndex}
        text = {
          dimensions[o.dimensionIndex].name 
            + ' / ' 
            + dimensions[o.dimensionIndex].dimensionLevels[o.dimensionLevelIndex].name
            + ' = '
            + dimensions[o.dimensionIndex].dimensionLevels[o.dimensionLevelIndex].possibleValues[o.valueIndex]
        }
        onDelete = { () => queryStore.removeFilter(i) }
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
      { queryStore.series.map((o, i) => (
        <DimensionChip
          key = {'series-' + o.dimensionIndex + '-' + o.dimensionLevelIndex}
          type = 'Series'
          text = {
            dimensions[o.dimensionIndex].name 
              + ' / ' 
              + dimensions[o.dimensionIndex].dimensionLevels[o.dimensionLevelIndex].name
          }
          zoomIn = { () => queryStore.zoomInSeries(i) }
          zoomOut = { () => queryStore.zoomOutSeries(i) }
          onDelete = { () => queryStore.removeSeries(i) }
        />
      )) }
      <AddDimensionChip type='Series' />
    </Grid>
  );
});

const Chart = observer(() => {
  const { queryStore } = useRootStore();
  return (
    <div style={{ width: '100%', height: '80vh', paddingTop: '20px' }}>
      <ResponsiveLine
        data = {queryStore.result.data}
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