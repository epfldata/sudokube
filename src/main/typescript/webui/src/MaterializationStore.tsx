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
  id: number;
  dimensions: {name: string, bits: string}[];
  constructor(id: number, dimensions: {name: string, bits: string}[]) {
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
  }
}
