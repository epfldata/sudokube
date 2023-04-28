import { makeAutoObservable, runInAction } from "mobx";
import { Cuboid } from "./MaterializationStore";

export class DimensionLevel {
  name: string;
  possibleValues: any[];
  constructor(name: string, possibleValues: any[]) {
    makeAutoObservable(this);
    this.name = name;
    this.possibleValues = possibleValues;
  }
}

export class TopLevelDimension {
  name: string;
  dimensionLevels: DimensionLevel[];
  constructor(name: string, dimensionLevels: DimensionLevel[]) {
    makeAutoObservable(this);
    this.name = name;
    this.dimensionLevels = dimensionLevels;
  }
}

export class DimensionHierarchy {
  dimensions: TopLevelDimension[];
  constructor(dimensions: TopLevelDimension[]) {
    makeAutoObservable(this);
    this.dimensions = dimensions;
  }
}

export type Dimension = {
  name: string;
  numBits: number;
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

export class QueryCube {
  dimensions: Dimension[] = [];
  dimensionHierarchy: DimensionHierarchy = new DimensionHierarchy([]);
  isLoading: boolean = false;
  constructor() {
    makeAutoObservable(this);
    this.loadDimensionHierarchy();
  }
  loadDimensionHierarchy() {
    const sampleMetadata = require('./sample-metadata.json');
    this.dimensionHierarchy = sampleMetadata.dimensionHierarchy;
    runInAction(() => this.dimensions = [
      { name: 'Country', numBits: 6 }, 
      { name: 'City', numBits: 6 },
      { name: 'Year', numBits: 6 },
      { name: 'Month', numBits: 5 }, 
      { name: 'Day', numBits: 6 }
    ]);
    // this.isLoading = true
    // this.transportLayer.fetchTodos().then(fetchedTodos => {
    //   runInAction(() => {
    //       fetchedTodos.forEach(json => this.updateTodoFromServer(json))
    //       this.isLoading = false
    //   })
    // })
  }
}

class ResultData {
  data: ResultDataSeries[];
  constructor(data: ResultDataSeries[]) {
    makeAutoObservable(this);
    this.data = data;
  }
}

class ResultDataSeries {
  id: string;
  data: ResultDataPoint[];
  constructor(id: string, data: ResultDataPoint[]) {
    makeAutoObservable(this);
    this.id = id;
    this.data = data;
  }
}

class ResultDataPoint {
  x: string;
  y: number;
  constructor(x: string, y: number) {
    makeAutoObservable(this);
    this.x = x;
    this.y = y;
  }
}

type Metric = {
  name: string;
  value: string;
}

export class QueryStore {
  cubes: string[] = [];
  selectedCubeIndex: number = 0;
  cube: QueryCube = new QueryCube();

  horizontal: SelectedDimension[] = [{dimensionIndex: 0, dimensionLevelIndex: 1}];
  series: SelectedDimension[] = [{dimensionIndex: 1, dimensionLevelIndex: 0}];
  filters: Filter[] = [{dimensionIndex: 2, dimensionLevelIndex: 0, valueIndex: 0}];

  measures: string[] = ['Sales'];
  measure: string = 'Sales';

  aggregations: string[] = ['SUM', 'AVG'];
  aggregation: string = 'SUM';

  solvers: string[] = ['Naive', 'Linear Programming', 'Moment', 'Graphical Model'];
  solver: string = 'Naive';

  modes: string[] = ['Batch', 'Online'];
  mode: string = 'Batch';

  isRunComplete: boolean = false;

  preparedCuboids: Cuboid[] = [];
  nextCuboidIndex: number = 0;

  result: ResultData = new ResultData([]);
  metrics: Metric[] = [];

  constructor() {
    makeAutoObservable(this);
  }
  addHorizontal(dimensionIndex: number, dimensionLevelIndex: number) {
    runInAction(() => this.horizontal.push(new SelectedDimension(dimensionIndex, dimensionLevelIndex)));
  }
  zoomInHorizontal(index: number) {
    runInAction(() => this.horizontal[index].dimensionLevelIndex = Math.min(
      this.horizontal[index].dimensionLevelIndex + 1,
      this.cube.dimensionHierarchy.dimensions[this.horizontal[index].dimensionIndex].dimensionLevels.length - 1
    ));
  }
  zoomOutHorizontal(index: number) {
    runInAction(() => this.horizontal[index].dimensionLevelIndex = Math.max(this.horizontal[index].dimensionLevelIndex - 1, 0));
  }
  addSeries(dimensionIndex: number, dimensionLevelIndex: number) {
    runInAction(() => this.series.push(new SelectedDimension(dimensionIndex, dimensionLevelIndex)));
  }
  zoomInSeries(index: number) {
    runInAction(() => this.series[index].dimensionLevelIndex = Math.min(
      this.series[index].dimensionLevelIndex + 1,
      this.cube.dimensionHierarchy.dimensions[this.series[index].dimensionIndex].dimensionLevels.length - 1
    ));
  }
  zoomOutSeries(index: number) {
    runInAction(() => this.series[index].dimensionLevelIndex = Math.max(this.series[index].dimensionLevelIndex - 1, 0));
  }
  addFilter(dimensionIndex: number, dimensionLevelIndex: number, valueIndex: number) {
    runInAction(() => this.filters.push(new Filter(dimensionIndex, dimensionLevelIndex, valueIndex)));
  }
}
