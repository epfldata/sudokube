import { makeAutoObservable, runInAction, reaction } from "mobx"
import { makePersistable } from "mobx-persist-store";

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

export class MetadataStore {
  dimensionHierarchy: DimensionHierarchy = new DimensionHierarchy([]);
  isLoading: boolean = false;
  test: number = 0;
  constructor() {
    makeAutoObservable(this);
    makePersistable(this, { name: 'MetadataStore', properties: ['test'], storage: window.localStorage });
    this.loadDimensionHierarchy();
  }
  testToggle() {
    this.test ^= 1;
  }
  get testNumber() {
    return this.test;
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
