import { makeAutoObservable } from "mobx";
import { MergeColumnDef } from "./_proto/sudokubeRPC_pb";

export class ExploreTransformStore {
  cubes: string[] = [];
  cube: string = '';
  dimensions: string[] = [];
  transformations: MergeColumnDef.AsObject[] = [
    {
      dim1: this.dimensions[0],
      dim2: this.dimensions[1],
      newDim: ''
    }
  ];
  constructor() {
    makeAutoObservable(this);
  }
}
