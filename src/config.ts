import { Config } from '@verdaccio/types';

export interface StorageConfig extends Config {
  containerName: string;
  keyPrefix: string;
  endpointSuffix: string;
  accountName: string;
  accountKey: string;
}
