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
export class ExploreTransformStore {
  cubes: string[] = [];
  selectedCubeIndex: number = 0;

  dimensions: ExploreDimension[] = [];

  checkRenameDimension1Index: number = 0;
  checkRenameDimension2Index: number = 0;

  checkRenameResult: boolean = false;

  findRenameTimeDimensionIndex: number = 0;
  timeNumBits: number = 0;
  timeBitsFilters: (0 | 1)[] = [];
  timeRange1: string = '0';
  timeRange2: string = '1';
  count1: number = 0;
  count2: number = 0;

  renameTime: string = '...';

  mergeDimension1Index: number = 0;
  mergeDimension2Index: number = 0;

  newCubeName: string = 'sales-merged';

  constructor() {
    makeAutoObservable(this);
  }
}
