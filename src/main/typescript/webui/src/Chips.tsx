import React, { ReactNode } from 'react'
import { Button, Chip, Grid } from '@mui/material'
import FilterAltIcon from '@mui/icons-material/FilterAlt';
import ArrowForwardIcon from '@mui/icons-material/ArrowForward';
import SsidChartIcon from '@mui/icons-material/SsidChart';
import AddIcon from '@mui/icons-material/Add';
import RemoveIcon from '@mui/icons-material/Remove';

export function InChipButton({ icon }: { icon: ReactNode }) {
  return (
    <Button style={{
      padding: "0px", border: "0px", margin: "0px", 
      height: "23px", minWidth: "16px", maxWidth: "16px", 
      display: "inline"
    }}>
      {icon}
    </Button>
  )
}
  
export const chipStyle = {
  height: "22px",
  marginLeft: "0px", marginRight: "5px", marginTop: "0px", marginBottom: "5px"
}

export function DimensionChip({ type, text, onDelete }: {
  type: "Horizontal" | "Series", 
  text: ReactNode, 
  onDelete: ((event: any) => void) | undefined
}) {
  return (<Chip
    style={chipStyle}
    variant="outlined"
    icon = { 
      type == "Horizontal" 
        ? <ArrowForwardIcon style = {{ height: "15px" }} /> 
        : <SsidChartIcon style = {{ height: "15px" }} />
    }
    label = { <span>
      {text} 
      <InChipButton icon = { <AddIcon style = {{ width: "16px", height: "16px", marginTop: "3px" }} /> } />
      <InChipButton icon = { <RemoveIcon style = {{ width: "16px", height: "16px", marginTop: "3px" }} /> } />
    </span> }
    onDelete = {onDelete}
  />)
}

export function AddDimensionChip({ type }: { type: "Horizontal" | "Series"}) {
  return (<Chip
    style={chipStyle}
    variant="outlined"
    icon = { 
      type == "Horizontal" 
        ? <ArrowForwardIcon style = {{ height: "15px" }} /> 
        : <SsidChartIcon style = {{ height: "15px" }} />
    }
    label = "Add ..."
    color = "primary"
    onClick = {() => {}}
  />)
}

export function FilterChip({ text, onDelete }: {
  text: ReactNode, onDelete: ((event: any) => void) | undefined 
}) {
  return (<Chip
    style = {chipStyle}
    variant = "outlined"
    icon = { <FilterAltIcon style = {{ height: "18px" }} /> }
    label = {text} 
    onDelete = {onDelete}
  />)
}

export function SelectionChip({ keyText, valueText }: {
  keyText: ReactNode, valueText: ReactNode 
}) {
  return (<Chip 
    style = {chipStyle}
    variant="outlined"
    label={<span><b>{keyText} </b>{valueText}</span>}
    onClick={()=>{}} 
  />)
}