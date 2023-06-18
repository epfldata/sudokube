import { makeAutoObservable } from 'mobx';
import React from 'react';
import { ReactNode, createContext, useContext } from 'react';
import { QueryStore } from './QueryStore';
import MaterializationStore from './MaterializationStore';
import { ExploreTransformStore } from './ExploreTransformStore';

export const apiBaseUrl = "http://sudokube.ddns.net:8081";

export class RootStore {
  readonly materializationStore: MaterializationStore;
  readonly exploreTransformStore: ExploreTransformStore;
  readonly queryStore: QueryStore;
  constructor() {
    makeAutoObservable(this);
    this.materializationStore = new MaterializationStore();
    this.exploreTransformStore = new ExploreTransformStore();
    this.queryStore = new QueryStore();
  }
}

const RootStoreContext = createContext<RootStore | undefined>(undefined);

export function useRootStore(): RootStore {
  const rootStore = useContext(RootStoreContext);
  if (!rootStore) {
    throw new Error('useRootStore must be used within RootStoreContextProvider');
  }
  return rootStore;
}

export function RootStoreContextProvider({children, rootStore}: {children?: ReactNode, rootStore: RootStore}) {
  return (
    <RootStoreContext.Provider value = {rootStore}>{children}</RootStoreContext.Provider>
  );
}