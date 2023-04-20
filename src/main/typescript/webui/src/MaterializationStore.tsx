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

export class Cuboid {
  id: string;
  dimensions: {name: string, bits: string}[];
  constructor(id: string, dimensions: {name: string, bits: string}[]) {
    makeAutoObservable(this);
    this.id = id;
    this.dimensions = dimensions;
  }
}

export default class MaterializationStore {
  datasets: string[] = [];
  selectedDataset: string = '';

  strategies: MaterializationStrategy[] = [];
  strategyIndex: number = 0;
  strategyParameters: string[] = [];

  dimensions: MaterializationDimension[] = [];

  addCuboidsFilters: MaterializationFilter[] = [];
  addCuboidsFilteredCuboids: Cuboid[] = [];

  chosenCuboidsFilters: MaterializationFilter[] = [];
  chosenCuboids: Cuboid[] = [];
  filteredChosenCuboids: Cuboid[] = [];

  constructor() {
    makeAutoObservable(this);
    this.loadDatasets();
    this.loadStrategies();
    this.loadCuboids();

    // TODO: Remove this when cuboids can really be added
    this.filteredChosenCuboids = this.chosenCuboids = [
      {
        id: "1",
        dimensions: [
          { name: "Country", bits: '\u2589\u2589\u2589\u25A2\u25A2\u25A2' },
          { name: "City", bits: '\u25A2\u25A2\u25A2\u25A2\u25A2\u25A2' },
          { name: "Year", bits: '\u25A2\u25A2\u25A2\u25A2\u25A2\u25A2' },
          { name: "Month", bits: '\u2589\u2589\u2589\u2589\u25A2' },
          { name: "Day", bits: '\u25A2\u25A2\u25A2\u25A2\u25A2\u25A2'}
        ] 
      }
    ]
  }
  loadDatasets() {
    // TODO: Call backend to fetch available datasets
    this.datasets = ['Sales'];
    this.setDatasetAndLoadDimensions(this.datasets[0]);
  }
  setDatasetAndLoadDimensions(dataset: string) {
    this.selectedDataset = dataset;
    this.loadDimensions(dataset);
  }
  loadStrategies() {
    // TODO: Call backend to fetch available strategies (with parameters)
    this.strategies = [new MaterializationStrategy('Strategy', [
      new MaterializationStrategyParameter('Parameter1', ['1', '2']),
      new MaterializationStrategyParameter('Parameter2')])];
  }
  setStrategyIndex(index: number) {
    this.strategyIndex = index;
    this.strategyParameters = Array(this.strategies[index].parameters.length).fill('');
  }
  setStrategyParameter(index: number, value: string) {
    this.strategyParameters[index] = value;
  }
  loadDimensions(dataset: string) {
    // TODO: Call backend to fetch available dimensions of the dataset
    this.dimensions = [
      new MaterializationDimension('Country', 6), 
      new MaterializationDimension('City', 6), 
      new MaterializationDimension('Year', 6), 
      new MaterializationDimension('Month', 5), 
      new MaterializationDimension('Day', 6)
    ];
  }

  loadCuboids() {
    // TODO: Call backend to fetch cuboids according to filters
    this.addCuboidsFilteredCuboids = [
      {
        id: "1",
        dimensions: [
          { name: "Country", bits: '\u2589\u2589\u2589\u25A2\u25A2\u25A2' },
          { name: "City", bits: '\u25A2\u25A2\u25A2\u25A2\u25A2\u25A2' },
          { name: "Year", bits: '\u25A2\u25A2\u25A2\u25A2\u25A2\u25A2' },
          { name: "Month", bits: '\u2589\u2589\u2589\u2589\u25A2' },
          { name: "Day", bits: '\u25A2\u25A2\u25A2\u25A2\u25A2\u25A2'}
        ] 
      }
    ]
  }
  addAddCuboidsFilter(dimensionIndex: number, bitsFrom: number, bitsTo: number) {
    this.addCuboidsFilters.push(new MaterializationFilter(dimensionIndex, bitsFrom, bitsTo));
  }
  removeAddCuboidsFilterAtIndex(index: number) {
    this.addCuboidsFilters.splice(index, 1);
  }
  setAddCuboidsFilteredCuboids(cuboids: Cuboid[]) {
    this.addCuboidsFilteredCuboids = cuboids;
  }

  addChosenCuboid(cuboids: Cuboid[]) {
    // TODO: Add cuboids selected
  }
  addChosenCuboidsFilter(dimensionIndex: number, bitsFrom: number, bitsTo: number) {
    this.chosenCuboidsFilters.push(new MaterializationFilter(dimensionIndex, bitsFrom, bitsTo));
  }
}
