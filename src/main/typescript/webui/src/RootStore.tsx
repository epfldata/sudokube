import { makeAutoObservable, runInAction } from 'mobx';
import React from 'react';
import { ReactNode, createContext, useContext } from 'react';
import { QueryStore } from './QueryStore';
import MaterializationStore from './MaterializationStore';

export class RootStore {
  readonly materializationStore: MaterializationStore;
  readonly queryStore: QueryStore;
  constructor() {
    makeAutoObservable(this);
    this.materializationStore = new MaterializationStore();
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