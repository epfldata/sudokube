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
    // TODO: Fetch cubes
    this.setCubes(['sales']);
    // TODO: Fetch dimensions
    this.setDimensions([
      new ExploreDimension('Country', 6), 
      new ExploreDimension('City', 6), 
      new ExploreDimension('Year', 6), 
      new ExploreDimension('Month', 5), 
      new ExploreDimension('Day', 6)
    ]);
    // TODO: Fetch number of bits for time
    this.setTimeNumBits(16);
    this.addTimeBitsFilters(0);
    this.addTimeBitsFilters(1);
  }
  setCubes(cubes: string[]) {
    runInAction(() => this.cubes = cubes);
  }
  setCubeIndex(index: number) {
    this.selectedCubeIndex = index;
  }
  setDimensions(dimensions: ExploreDimension[]) {
    runInAction(() => this.dimensions = dimensions);
  }
  setCheckRenameDimension1Index(index: number) {
    this.checkRenameDimension1Index = index;
  }
  setCheckRenameDimension2Index(index: number) {
    this.checkRenameDimension2Index = index;
  }
  setCheckRenameResult(renamed: boolean) {
    this.checkRenameResult = renamed;
  }
  setFindRenameTimeDimensionIndex(index: number) {
    this.findRenameTimeDimensionIndex = index;
  }
  setTimeNumBits(numBits: number) {
    this.timeNumBits = numBits;
  }
  clearTimeBitsFilters() {
    runInAction(() => this.timeBitsFilters = []);
  }
  addTimeBitsFilters(value: 0 | 1) {
    runInAction(() => this.timeBitsFilters.push(value));
  }
  setRenameTime(renameTime: string) {
    this.renameTime = renameTime;
  }
  setMergeDimension1Index(index: number) {
    this.mergeDimension1Index = index;
  }
  setMergeDimension2Index(index: number) {
    this.mergeDimension2Index = index;
  }
  setNewCubeName(name: string) {
    this.newCubeName = name;
  }
}
