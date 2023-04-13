import { makeAutoObservable } from "mobx";

export class SelectedDimension {
  dimensionIndex: number;
  dimensionLevelIndex: number;
  constructor(dimensionIndex: number, dimensionLevelIndex: number) {
    makeAutoObservable(this);
    this.dimensionIndex = dimensionIndex;
    this.dimensionLevelIndex = dimensionLevelIndex;
  }
}

export class Filter {
  dimensionIndex: number;
  dimensionLevelIndex: number;
  valueIndex: number;
  constructor(dimensionIndex: number, dimensionLevelIndex: number, valueIndex: number) {
    makeAutoObservable(this);
    this.dimensionIndex = dimensionIndex;
    this.dimensionLevelIndex = dimensionLevelIndex;
    this.valueIndex = valueIndex;
  }
}

export class QueryInput {
  horizontal: SelectedDimension[];
  series: SelectedDimension[];
  filters: Filter[];
  constructor() {
    this.horizontal = [{dimensionIndex: 0, dimensionLevelIndex: 1}];
    this.series = [{dimensionIndex: 1, dimensionLevelIndex: 0}];
    this.filters = [{dimensionIndex: 2, dimensionLevelIndex: 0, valueIndex: 0}];
    makeAutoObservable(this);
  }
  removeHorizontal(index: number) {
    this.horizontal.splice(index, 1);
  }
  removeSeries(index: number) {
    this.series.splice(index, 1);
  }
  removeFilter(index: number) {
    this.filters.splice(index, 1);
  }
}

export class InputStore {
  queryInput: QueryInput;
  constructor() {
    makeAutoObservable(this);
    this.queryInput = new QueryInput();
  }
}