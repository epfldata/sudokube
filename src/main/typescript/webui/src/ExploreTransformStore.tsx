import { makeAutoObservable, runInAction } from "mobx";

export class ExploreDimension {
  name: string;
  numBits: number;
  constructor(name: string, numBits: number) {
    makeAutoObservable(this);
    this.name = name;
    this.numBits = numBits;
  }
}

export type TimeRangeCountRow = {
  range: string;
  nullCount: number;
  notNullCount: number;
}

export type DimensionNullCount = {
  dimension: string;
  nullCount: number;
  notNullCount: number;
}

export class ExploreTransformStore {
  cubes: string[] = [];
  selectedCubeIndex: number = 0;

  dimensions: ExploreDimension[] = [];

  checkRenameDimension1Index: number = 0;
  checkRenameDimension2Index: number = 0;

  dimensionsNullCount: DimensionNullCount[] = [
    { dimension: 'Country', nullCount: 0, notNullCount: 0 },
    { dimension: 'City', nullCount: 0, notNullCount: 0 }
  ]
  checkRenameResult: boolean = false;

  findRenameTimeDimensionIndex: number = 0;
  timeNumBits: number = 0;
  timeBitsFilters: (0 | 1)[] = [];
  timeRangeCounts: TimeRangeCountRow[] = [
    {range: '0 to 31', nullCount: 0, notNullCount: 0},
    {range: '32 to 63', nullCount: 0, notNullCount: 0}
  ]

  renameTime: string = '...';

  mergeDimension1Index: number = 0;
  mergeDimension2Index: number = 0;

  newCubeName: string = '';

  constructor() {
    makeAutoObservable(this);
  }
}
