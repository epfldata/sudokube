import React, { ReactNode } from 'react'
import { Button, Chip, Dialog, DialogActions, DialogContent, DialogTitle, FormControl, FormLabel, Grid, Input, InputLabel, MenuItem, Select, TextField } from '@mui/material'
import FilterAltIcon from '@mui/icons-material/FilterAlt';
import ArrowForwardIcon from '@mui/icons-material/ArrowForward';
import SsidChartIcon from '@mui/icons-material/SsidChart';
import AddIcon from '@mui/icons-material/Add';
import RemoveIcon from '@mui/icons-material/Remove';
import { useRootStore } from './RootStore';
import { chipStyle, InChipButton } from './GenericChips';
import { observer } from 'mobx-react-lite';

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
  const { queryStore } = useRootStore();
  const dimensionsHierarchy = queryStore.cube.dimensionHierarchy;
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
            { dimensionsHierarchy.dimensions.map((dimension, index) => (
              <MenuItem key = { 'add-dimension-select-dimension-' + index } value = {index}> {dimension.name} </MenuItem>
            )) }
          </Select>
        </FormControl>
        <FormControl sx={{ m: 1, minWidth: 120 }}>
          <InputLabel htmlFor="add-dimension-select-dimension-level">Dimension Level</InputLabel>
          <Select
            value = {dimensionLevelIndex}
            onChange = { e => setDimensionLevelIndex(e.target.value as number) }
            id="add-dimension-select-dimension-level" label="Dimension Level">
            { dimensionsHierarchy.dimensions[dimensionIndex].dimensionLevels.map((level, index) => (
              <MenuItem key = { 'add-dimension-select-dimension-level-' + index } value = {index}>{level.name}</MenuItem>
            )) }
          </Select>
        </FormControl>
        <DialogActions>
          <Button onClick = { () => setDialogOpen(false) }>Cancel</Button>
          <Button onClick = { () => { 
            switch (type) {
              case 'Horizontal': queryStore.addHorizontal(dimensionIndex, dimensionLevelIndex); break;
              case 'Series': queryStore.addSeries(dimensionIndex, dimensionLevelIndex); break;
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

export const AddQueryFilterChip = observer(() => {
  const { queryStore } = useRootStore();
  const [isDialogOpen, setDialogOpen] = React.useState(false);
  const [dimensionIndex, setDimensionIndex] = React.useState(0);
  const [dimensionLevelIndex, setDimensionLevelIndex] = React.useState(0);
  const [valueIndex, setValueIndex] = React.useState(0);
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
        <FormControl sx={{ m: 1, minWidth: 120 }}>
          <InputLabel htmlFor="add-query-filter-select-dimension">Dimension</InputLabel>
          <Select
            value = {dimensionIndex}
            onChange = { e => setDimensionIndex(e.target.value as number) }
            id="add-query-filter-select-dimension" label="Dimension">
            { queryStore.cube.dimensionHierarchy.dimensions.map((dimension, index) => (
              <MenuItem key = { 'add-query-filter-select-dimension-' + index } value = {index}> {dimension.name} </MenuItem>
            )) }
          </Select>
        </FormControl>
        <FormControl sx={{ m: 1, minWidth: 120 }}>
          <InputLabel htmlFor="add-query-filter-select-dimension-level">Dimension Level</InputLabel>
          <Select
            value = {dimensionLevelIndex}
            onChange = { e => {
              setDimensionLevelIndex(e.target.value as number);
              // TODO: Possibly fetch possible values here
            }}
            id="add-query-filter-select-dimension-level" label="Dimension Level">
            { queryStore.cube.dimensionHierarchy.dimensions[dimensionIndex].dimensionLevels.map((dimensionLevel, index) => (
              <MenuItem key = { 'add-query-filter-select-dimension-' + dimensionIndex + '-level-' + index } value = {index}> {dimensionLevel.name} </MenuItem>
            )) }
          </Select>
        </FormControl>
        <FormControl sx={{ m: 1, minWidth: 120 }}>
          <InputLabel htmlFor="add-query-filter-select-value">Value</InputLabel>
          <Select
            value = {valueIndex}
            onChange = { e => setValueIndex(e.target.value as number) }
            id = "add-query-filter-select-value" label = "Value">
            { queryStore.cube.dimensionHierarchy.dimensions[dimensionIndex].dimensionLevels[dimensionLevelIndex].possibleValues.map((value, index) => (
              <MenuItem 
                key = { 'add-query-filter-select-dimension-' + dimensionIndex + '-level-' + dimensionLevelIndex + '-value-' + index } 
                value = {index}
              > {value} </MenuItem>
            )) }
          </Select>
        </FormControl>
        <DialogActions>
          <Button onClick = { () => setDialogOpen(false) }>Cancel</Button>
          <Button onClick = { () => {
            queryStore.addFilter(dimensionIndex, dimensionLevelIndex, valueIndex);
            setDialogOpen(false);
          } }>Add</Button>
        </DialogActions>
      </DialogContent>
    </Dialog>
  </span>)
})