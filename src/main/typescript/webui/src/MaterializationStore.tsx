import { makeAutoObservable } from "mobx";

export class MaterializationDimension {
  name: string;
  numBits: number;
  constructor(name: string, numBits: number) {
    makeAutoObservable(this);
    this.name = name;
    this.numBits = numBits;
  }
}

export class MaterializationStrategyParameter {
  name: string;
  possibleValues?: string[];
  constructor(name: string, possibleValues?: string[]) {
    makeAutoObservable(this);
    this.name = name;
    if (possibleValues) {
      this.possibleValues = possibleValues;
    }
  }
}

export class MaterializationStrategy {
  name: string;
  parameters: MaterializationStrategyParameter[];
  constructor(name: string, parameters: MaterializationStrategyParameter[]) {
    makeAutoObservable(this);
    this.name = name;
    this.parameters = parameters;
  }
}

export class MaterializationFilter {
  dimensionIndex: number;
  bitsFrom: number;
  bitsTo: number;
  constructor(dimensionIndex: number, bitsFrom: number, bitsTo: number) {
    makeAutoObservable(this);
    this.dimensionIndex = dimensionIndex;
    this.bitsFrom = bitsFrom;
    this.bitsTo = bitsTo;
  }
}

export default class MaterializationStore {
  dataset: string = '';
  datasets: string[] = [];
  dimensions: MaterializationDimension[] = [];
  strategies: MaterializationStrategy[] = [];
  addCuboidsFilters: MaterializationFilter[] = [];
  constructor() {
    makeAutoObservable(this);
    this.loadDatasets();
    this.loadStrategies();
  }
  loadDatasets() {
    this.datasets = ['Sales'];
    this.setDatasetAndLoadDimensions(this.datasets[0]);
  }
  setDatasetAndLoadDimensions(dataset: string) {
    this.dataset = dataset;
    this.loadDimensions(dataset);
  }
  loadStrategies() {
    this.strategies = [new MaterializationStrategy('Strategy', [new MaterializationStrategyParameter('Parameter', ['1', '2'])])];
  }
  loadDimensions(dataset: string) {
    this.dimensions = [new MaterializationDimension('Country', 6), new MaterializationDimension('City', 6)];
  }
  addAddCuboidsFilter(dimensionIndex: number, bitsFrom: number, bitsTo: number) {
    this.addCuboidsFilters.push(new MaterializationFilter(dimensionIndex, bitsFrom, bitsTo));
  }
  removeFilterAtIndex(index: number) {
    this.addCuboidsFilters.splice(index, 1);
  }
}
