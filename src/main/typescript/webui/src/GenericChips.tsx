import { Button, Chip, Dialog, DialogActions, DialogContent, DialogTitle, FormControl, InputLabel, MenuItem, Select } from "@mui/material"
import React, { ReactNode } from "react"

export function InChipButton({ icon, onClick }: { icon: ReactNode, onClick: () => void }) {
  return (
    <Button 
      style={{
        padding: '0px', border: '0px', margin: '0px', 
        height: '23px', minWidth: '16px', maxWidth: '16px', 
        display: 'inline'
      }}
      onClick = {onClick}>
      {icon}
    </Button>
  )
}
  
export const chipStyle = {
  height: '22px',
  marginLeft: '0px', marginRight: '5px', marginTop: '0px', marginBottom: '5px'
}

export function SelectionChip({ keyText, valueText, valueRange, onChange }: {
  keyText: ReactNode, valueText: ReactNode, valueRange: string[], onChange: (value: string) => void
}) {
  const [isDialogOpen, setDialogOpen] = React.useState(false);
  return (<span>
    <Chip 
      style = {chipStyle}
      variant = 'outlined'
      label = { <span><b>{keyText} </b>{valueText}</span> }
      onClick = { () => { setDialogOpen(true) } } 
    />
    <Dialog open = {isDialogOpen}>
      <DialogTitle> { "Select " + keyText } </DialogTitle>
      <DialogContent>
        <FormControl sx = {{ m: 1, minWidth: 120 }}>
          <InputLabel htmlFor = { "select-" + keyText }> {keyText} </InputLabel>
          <Select
            value = {valueText}
            onChange = { e => {
              valueText = e.target.value;
              onChange(valueText as string);
              setDialogOpen(false);
            } }
            id = { 'select-' + keyText } label = {keyText}>
            { valueRange.map(value => (
              <MenuItem key = { 'select-' + keyText + '-' + value } value = {value}> {value} </MenuItem>
            )) }
          </Select>
        </FormControl>
        <DialogActions>
          <Button onClick = { () => setDialogOpen(false) }>Cancel</Button>
        </DialogActions>
      </DialogContent>
    </Dialog>
  </span>)
}

export function ButtonChip({ label, variant, onClick }: {
  label: string, 
  variant: 'outlined' | 'filled'
  onClick: React.MouseEventHandler<HTMLDivElement>
}) {
  return (
    <Chip
      label = {label}
      style = {chipStyle}
      variant = {variant}
      color = 'primary'
      onClick = {onClick}
    />
  )
}
