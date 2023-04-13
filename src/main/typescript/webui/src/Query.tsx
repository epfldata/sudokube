import * as React from 'react';
import Container from '@mui/material/Container';
import { Chip, Grid } from '@mui/material'
import FilterAltIcon from '@mui/icons-material/FilterAlt';
import { DimensionChip, AddDimensionChip, FilterChip, SelectionChip, chipStyle } from './Chips';
import { ResponsiveLine } from '@nivo/line';
import { observer } from 'mobx-react-lite';
import { useRootStore } from './RootStore';

const data = require('./sales-data.json');

export default observer(function Query() {
  const { metadataStore, inputStore } = useRootStore();
  const dimensions = metadataStore.dimensionHierarchy.dimensions;

  return (
    <Container style={{ paddingTop: '20px' }}>
      <Grid container maxHeight='30vh' overflow='scroll' style={{ paddingTop: '1px', paddingBottom: '1px' }}>
        <Grid item xs={6}>
          { inputStore.queryInput.horizontal.map((o, i) => (<DimensionChip
            key = {'horizontal-' + o.dimensionIndex + '-' + o.dimensionLevelIndex}
            type = 'Horizontal'
            text = {
              dimensions[o.dimensionIndex].name 
                + ' / ' 
                + dimensions[o.dimensionIndex].dimensionLevels[o.dimensionLevelIndex].name
            }
            onDelete = { () => inputStore.queryInput.removeHorizontal(i) }
          />)) }
          <AddDimensionChip type='Horizontal' />
        </Grid>
        <Grid item xs={6}>
          { inputStore.queryInput.filters.map((o, i) => (<FilterChip
            key = {'filter-' + o.dimensionIndex + '-' + o.dimensionLevelIndex + '-' + o.valueIndex}
            text = {
              dimensions[o.dimensionIndex].name 
                + ' / ' 
                + dimensions[o.dimensionIndex].dimensionLevels[o.dimensionLevelIndex].name
                + ' = '
                + dimensions[o.dimensionIndex].dimensionLevels[o.dimensionLevelIndex].possibleValues[o.valueIndex]
            }
            onDelete = { () => inputStore.queryInput.removeFilter(i) }
          />)) }
          <Chip
            icon={<FilterAltIcon style={{ height: '18px' }} />}
            label='Add ...'
            onClick={() => { }}
            style={chipStyle}
            variant='outlined'
            color='primary'
          />
        </Grid>
        <Grid item xs={6}>
          { inputStore.queryInput.series.map((o, i) => (<DimensionChip
            key = {'series-' + o.dimensionIndex + '-' + o.dimensionLevelIndex}
            type = 'Series'
            text = {
              dimensions[o.dimensionIndex].name 
                + ' / ' 
                + dimensions[o.dimensionIndex].dimensionLevels[o.dimensionLevelIndex].name
            }
            onDelete = { () => inputStore.queryInput.removeSeries(i) }
          />)) }
          <AddDimensionChip type='Series' />
        </Grid>
        <Grid item xs={6}>
          <SelectionChip keyText='Measure' valueText='Sales' />
          <SelectionChip keyText='Aggregation' valueText='SUM' />
          <SelectionChip keyText='Solver' valueText='Naive' />
          <Chip
            label='Run'
            style={chipStyle}
            variant='filled'
            color='primary'
            onClick={() => { }}
          />
        </Grid>
      </Grid>
      <div style={{ width: '100%', height: '80vh', paddingTop: '20px' }}>
        <ResponsiveLine
          data={data.data}
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
          // axisBottom={{
          //   // orient: 'bottom',
          //   tickSize: 5,
          //   tickPadding: 5,
          //   tickRotation: 0,
          //   legend: 'transportation',
          //   legendOffset: 36,
          //   legendPosition: 'middle'
          // }}
          // axisLeft={{
          //   // orient: 'left',
          //   tickSize: 5,
          //   tickPadding: 5,
          //   tickRotation: 0,
          //   legend: 'count',
          //   legendOffset: -40,
          //   legendPosition: 'middle'
          // }}
          // pointSize={10}
          // pointColor={{ theme: 'background' }}
          // pointBorderWidth={2}
          // pointBorderColor={{ from: 'serieColor' }}
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
    </Container>
  )
})