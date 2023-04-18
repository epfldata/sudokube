import { makeAutoObservable, runInAction } from 'mobx';
import React from 'react';
import { ReactNode, createContext, useContext } from 'react';
import { QueryStore } from './QueryStore';
import { MaterializationStore } from './MaterializationStore';

export class RootStore {
  readonly queryStore: QueryStore;
  readonly materializationStore: MaterializationStore;
  constructor() {
    makeAutoObservable(this);
    this.queryStore = new QueryStore();
    this.materializationStore = new MaterializationStore();
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