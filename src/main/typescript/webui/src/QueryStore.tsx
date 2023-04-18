import { makeAutoObservable } from "mobx";

export class DimensionLevel {
  name: string;
  possibleValues: any[];
  constructor(name: string, possibleValues: any[]) {
    makeAutoObservable(this);
    this.name = name;
    this.possibleValues = possibleValues;
  }
}

export class Dimension {
  name: string;
  dimensionLevels: DimensionLevel[];
  constructor(name: string, dimensionLevels: DimensionLevel[]) {
    makeAutoObservable(this);
    this.name = name;
    this.dimensionLevels = dimensionLevels;
  }
}

export class DimensionHierarchy {
  dimensions: Dimension[];
  constructor(dimensions: Dimension[]) {
    makeAutoObservable(this);
    this.dimensions = dimensions;
  }
}

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

export class QueryMetadata {
  dimensionHierarchy: DimensionHierarchy = new DimensionHierarchy([]);
  isLoading: boolean = false;
  constructor() {
    makeAutoObservable(this);
    this.loadDimensionHierarchy();
  }
  loadDimensionHierarchy() {
    const sampleMetadata = require('./sample-metadata.json');
    this.dimensionHierarchy = sampleMetadata.dimensionHierarchy;
    // this.isLoading = true
    // this.transportLayer.fetchTodos().then(fetchedTodos => {
    //   runInAction(() => {
    //       fetchedTodos.forEach(json => this.updateTodoFromServer(json))
    //       this.isLoading = false
    //   })
    // })
  }
}

export class QueryInputs {
  horizontal: SelectedDimension[];
  series: SelectedDimension[];
  filters: Filter[];
  constructor() {
    this.horizontal = [{dimensionIndex: 0, dimensionLevelIndex: 1}];
    this.series = [{dimensionIndex: 1, dimensionLevelIndex: 0}];
    this.filters = [{dimensionIndex: 2, dimensionLevelIndex: 0, valueIndex: 0}];
    makeAutoObservable(this);
  }
  addHorizontal(dimensionIndex: number, dimensionLevelIndex: number) {
    this.horizontal.push(new SelectedDimension(dimensionIndex, dimensionLevelIndex));
  }
  removeHorizontal(index: number) {
    this.horizontal.splice(index, 1);
  }
  addSeries(dimensionIndex: number, dimensionLevelIndex: number) {
    this.series.push(new SelectedDimension(dimensionIndex, dimensionLevelIndex));
  }
  removeSeries(index: number) {
    this.series.splice(index, 1);
  }
  removeFilter(index: number) {
    this.filters.splice(index, 1);
  }
}

export class QueryStore {
  metadata: QueryMetadata;
  inputs: QueryInputs;
  constructor() {
    this.metadata = new QueryMetadata();
    this.inputs = new QueryInputs();
  }
}
