import * as React from 'react';
import { Dialog, DialogTitle, DialogContent, DialogActions, Button } from "@mui/material";
import { useRootStore } from "./RootStore";
import { runInAction } from "mobx";
import { observer } from 'mobx-react-lite';
import { useCallback } from 'react';

export const ErrorPopup = observer(() => {
  const { errorStore } = useRootStore();
  return (
    <Dialog open = {errorStore.isErrorPopupOpen}>
      <DialogTitle>Error</DialogTitle>
      <DialogContent>{errorStore.errorMessage}</DialogContent>
      <DialogActions>
        <Button onClick = { () => runInAction(() => errorStore.isErrorPopupOpen = false) }>Done</Button>
      </DialogActions>
    </Dialog>
  );
});
