import { makeAutoObservable } from "mobx";
import { makePersistable } from "mobx-persist-store";

export class MaterializationDimension {
  name: string;
  numBits: number;
  constructor(name: string, numBits: number) {
    makeAutoObservable(this);
    this.name = name;
    this.numBits = numBits;
  }
}

export class MaterializationMetadata {
  dimensions: MaterializationDimension[];
  constructor() {
    makeAutoObservable(this);
    this.dimensions = [{name: 'Country', numBits: 6}];
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

export class MaterializationInput {
  filters: MaterializationFilter[];
  constructor() {
    makeAutoObservable(this);
    this.filters = [{dimensionIndex: 0, bitsFrom: 5, bitsTo: 5}];
  }
}

export class MaterializationStore {
  metadata: MaterializationMetadata;
  input: MaterializationInput;
  constructor() {
    makeAutoObservable(this);
    makePersistable(this, { name: 'MaterializationStore', properties: ['input'], storage: window.sessionStorage });
    this.metadata = new MaterializationMetadata();
    this.input = new MaterializationInput();
  }
  addFilter(dimension: string, bitsFrom: number, bitsTo: number) {
    this.input.filters.push(new MaterializationFilter(1, bitsFrom, bitsTo));
  }
  removeFilterAtIndex(index: number) {
    this.input.filters.splice(index, 1);
  }
}
