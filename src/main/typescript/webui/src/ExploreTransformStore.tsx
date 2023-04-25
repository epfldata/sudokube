import { makeAutoObservable } from "mobx";

export type ExploreDimension = {
  name: string;
  numBits: number;
}

export class ExploreTransformStore {
  dimensions: ExploreDimension[] = [];
  timeNumBits: number = 0;
  constructor() {
    makeAutoObservable(this);
  }
}
