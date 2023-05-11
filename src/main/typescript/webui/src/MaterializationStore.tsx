import { makeAutoObservable } from "mobx";
import { CuboidDef, DimensionFilterCuboid } from "./_proto/sudokubeRPC_pb";

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

  addCuboidsFilters: DimensionFilterCuboid[] = [];

  chosenCuboids: CuboidDef[] = [];
  chosenCuboidsFilters: DimensionFilterCuboid[] = [];
  chosenCuboidsPage: number = 0;
  chosenCuboidsPageSize: number = 10;

  constructor() {
    makeAutoObservable(this);
  }
}
