import { makeAutoObservable, runInAction } from 'mobx';
import { MetadataStore } from './Metadata';
import React from 'react';
import { ReactNode, createContext, useContext } from 'react';
import { InputStore } from './InputStore';

export class RootStore {
  readonly metadataStore: MetadataStore;
  readonly inputStore: InputStore;
  constructor() {
    makeAutoObservable(this);
    this.metadataStore = new MetadataStore();
    this.inputStore = new InputStore();
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