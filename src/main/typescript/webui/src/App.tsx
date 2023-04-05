import * as React from 'react';
import Container from '@mui/material/Container';
import Typography from '@mui/material/Typography';
import { Chip, Grid } from '@mui/material'
import FilterAltIcon from '@mui/icons-material/FilterAlt';
import { DimensionChip, AddDimensionChip, FilterChip, SelectionChip, chipStyle } from './Chips';
import AppBar from '@mui/material/AppBar';
import Toolbar from '@mui/material/Toolbar';
import { ResponsiveLine } from '@nivo/line';
const data = require('./example-data.json');

export default function App() {
  return (
    <Container style = {{ maxWidth: "100vw", padding: "0"  }}>
      <AppBar position = "static" style = {{ backgroundColor: "#212121" }}>
        <Toolbar variant = "dense">
          <Typography variant = "h6" component = "div" sx = {{ flexGrow: 1 }}>
            Sudokube
          </Typography>
        </Toolbar>
      </AppBar>
      <Container style = {{ paddingTop: "20px" }}>
        <Grid container maxHeight = "30vh" overflow = "scroll" style = {{ paddingTop: "1px", paddingBottom: "1px" }}>
          <Grid item xs = {6}>
            <DimensionChip type = "Horizontal" text = "Time / Month" onDelete = {()=>{}} />
            <AddDimensionChip type = "Horizontal" />
          </Grid>
          <Grid item xs = {6}>
            <FilterChip text = "Product / Category = Sports" onDelete = {()=>{}} />
            <Chip
              icon = { <FilterAltIcon style = {{ height: "18px" }} /> }
              label="Add ..."
              onClick={()=>{}}
              style={chipStyle}
              variant="outlined"
              color="primary" 
            />
          </Grid>
          <Grid item xs = {6}>
            <DimensionChip type = "Series" text = "Location / Country" onDelete = { () => {} } />
            <AddDimensionChip type = "Series" />
          </Grid>
          <Grid item xs = {6}>
            <SelectionChip keyText = "Measure" valueText = "Sales" />
            <SelectionChip keyText = "Aggregation" valueText = "SUM" />
            <SelectionChip keyText = "Solver" valueText = "Naive" />
            <Chip
              label = "Run"
              style = {chipStyle}
              variant = "filled"
              color = "primary"
              onClick = {() => {}} 
            />
          </Grid>
        </Grid>
        <div style = {{ width: "100%", height: "80vh", paddingTop: "20px" }}>
          <ResponsiveLine
            data = {data.data}
            margin = {{ top: 5, right: 100, bottom: 25, left: 30 }}
            xScale = {{ type: 'point' }}
            yScale = {{
              type: 'linear',
              min: 'auto',
              max: 'auto',
              stacked: false,
              reverse: false
            }}
            yFormat = " >-.2f"
            axisTop = {null}
            axisRight = {null}
            // axisBottom={{
            //     // orient: 'bottom',
            //     tickSize: 5,
            //     tickPadding: 5,
            //     tickRotation: 0,
            //     legend: 'transportation',
            //     legendOffset: 36,
            //     legendPosition: 'middle'
            // }}
            // axisLeft={{
            //     // orient: 'left',
            //     tickSize: 5,
            //     tickPadding: 5,
            //     tickRotation: 0,
            //     legend: 'count',
            //     legendOffset: -40,
            //     legendPosition: 'middle'
            // }}
            // pointSize={10}
            // pointColor={{ theme: 'background' }}
            // pointBorderWidth={2}
            // pointBorderColor={{ from: 'serieColor' }}
            pointLabelYOffset = {-12}
            useMesh = {true}
            legends = {[
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
    </Container>
  );
}
