import { makeAutoObservable } from "mobx";

export class ErrorStore {
  isErrorPopupOpen: boolean = false;
  errorMessage: string = "";
  constructor() {
    makeAutoObservable(this);
  }
}
