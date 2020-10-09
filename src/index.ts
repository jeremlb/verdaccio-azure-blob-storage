import {
  LocalStorage,
  Logger,
  Config,
  Callback,
  IPluginStorage,
  PluginOptions,
  Token,
  TokenFilter,
} from '@verdaccio/types';
import { getInternalError, getServiceUnavailable } from '@verdaccio/commons-api';

import { StorageConfig } from './config';
import StoragePackageManager from './storage-paquet-manager';
import { addTrailingSlash, setConfigValue, streamToBuffer } from './utils';
import { BlobServiceClient, ContainerClient, StorageSharedKeyCredential,  } from '@azure/storage-blob';

export default class S3Database implements IPluginStorage<StorageConfig> {
  public logger: Logger;
  public config: StorageConfig;
  private container: ContainerClient;
  private _localData: LocalStorage | null;

  public constructor(config: Config, options: PluginOptions<StorageConfig>) {
    this.logger = options.logger;
    // copy so we don't mutate
    if (!config) {
      throw new Error('blob storage missing config. Add `store.azure-blob-storage` to your config file');
    }
    this.config = Object.assign(config, config.store['azure-blob-storage']);

    if (!this.config.container) {
      throw new Error('container storage requires a bucket');
    }

    this.config.container = setConfigValue(this.config.container);
    this.config.keyPrefix = setConfigValue(this.config.keyPrefix);
    this.config.endpointSuffix = setConfigValue(this.config.endpointSuffix);
    this.config.accountName = setConfigValue(this.config.accountName);
    this.config.accountKey = setConfigValue(this.config.accountKey);

    const configKeyPrefix = this.config.keyPrefix;
    this._localData = null;
    this.config.keyPrefix = addTrailingSlash(configKeyPrefix);

    this.logger.debug({ config: JSON.stringify(this.config, null, 4) }, 'blob: configuration: @{config}');

    const creds = new StorageSharedKeyCredential(this.config.accountName, this.config.accountKey);
    const blob = new BlobServiceClient(`https://${this.config.accountName}.blob.${this.config.endpointSuffix}`, creds);

    this.container = blob.getContainerClient(this.config.containerName);
  }

  public async getSecret(): Promise<string> {
    return Promise.resolve((await this._getData()).secret);
  }

  public async setSecret(secret: string): Promise<void> {
    (await this._getData()).secret = secret;
    await this._sync();
  }

  public add(name: string, callback: Callback): void {
    this.logger.debug({ name }, 'blob: [add] private package @{name}');
    this._getData().then(async data => {
      if (data.list.indexOf(name) === -1) {
        data.list.push(name);
        this.logger.trace({ name }, 'blob: [add] @{name} has been added');
        try {
          await this._sync();
          callback(null);
        } catch (err) {
          callback(err);
        }
      } else {
        callback(null);
      }
    });
  }

  public async search(onPackage: Function, onEnd: Function): Promise<void> {
    this.logger.debug('blob: [search]');
    const storage = await this._getData();
    const storageInfoMap = storage.list.map(this._fetchPackageInfo.bind(this, onPackage));
    this.logger.debug({ l: storageInfoMap.length }, 'blob: [search] storageInfoMap length is @{l}');
    await Promise.all(storageInfoMap);
    onEnd();
  }

  private async _fetchPackageInfo(onPackage: Function, packageName: string): Promise<void> {
    const { keyPrefix } = this.config;
    this.logger.debug({ packageName }, 'blob: [_fetchPackageInfo] @{packageName}');
    return new Promise((resolve): void => {
      const blob = this.container.getBlobClient(`${keyPrefix + packageName}/package.json`);
      blob.getProperties()
        .then(value => {
          const lastModified = value.lastModified;
          if (lastModified) {

            this.logger.trace({ lastModified }, 'blob: [_fetchPackageInfo] lastModified: @{lastModified}');
            return onPackage(
              {
                name: packageName,
                path: packageName,
                time: lastModified.getTime(),
              },
              resolve
            );
          }
          resolve();
        })
        .catch((err) => {
          this.logger.debug({ err }, 'blob: [_fetchPackageInfo] error: @{err}');
          resolve();
        });
    });
  }

  public remove(name: string, callback: Callback): void {
    this.logger.debug({ name }, 'blob: [remove] @{name}');
    this.get(async (err, data) => {
      if (err) {
        this.logger.error({ err }, 'blob: [remove] error: @{err}');
        callback(getInternalError('something went wrong on remove a package'));
      }

      const pkgName = data.indexOf(name);
      if (pkgName !== -1) {
        const data = await this._getData();
        data.list.splice(pkgName, 1);
        this.logger.debug({ pkgName }, 'blob: [remove] sucessfully removed @{pkgName}');
      }

      try {
        this.logger.trace('blob: [remove] starting sync');
        await this._sync();
        this.logger.trace('blob: [remove] finish sync');
        callback(null);
      } catch (err) {
        this.logger.error({ err }, 'blob: [remove] sync error: @{err}');
        callback(err);
      }
    });
  }

  public get(callback: Callback): void {
    this.logger.debug('blob: [get]');
    this._getData().then(data => callback(null, data.list));
  }

  // Create/write database file to storage
  private async _sync(): Promise<void> {
    await new Promise(async (resolve, reject): Promise<void> => {
      const { keyPrefix } = this.config;

      const blob = this.container.getBlobClient(`${keyPrefix}verdaccio-blob-db.json`);
      const client = blob.getBlockBlobClient();
      const content = JSON.stringify(this._localData);

      try {
        await client.upload(content, content.length);
        this.logger.debug('blob: [_sync] sucess');
        resolve();
      } catch (e) {
        this.logger.error({ e }, 'blob: [_sync] error: @{err}');
        reject(e);
        return;
      }
    });
  }

  // returns an instance of a class managing the storage for a single package
  public getPackageStorage(packageName: string): StoragePackageManager {
    this.logger.debug({ packageName }, 'blob: [getPackageStorage] @{packageName}');

    return new StoragePackageManager(this.config, packageName, this.logger);
  }

  private async _getData(): Promise<LocalStorage> {
    if (!this._localData) {
      this._localData = await new Promise(async (resolve, reject): Promise<void> => {
        const { keyPrefix } = this.config;
        this.logger.trace('blob: [_getData] get database object');
        const blob = this.container.getBlobClient(`${keyPrefix}verdaccio-blob-db.json`);
        const client = blob.getBlockBlobClient();

        try {
          const downloadBlockBlobResponse = await client.download();

          if (downloadBlockBlobResponse._response.status === 404) {
              this.logger.error('blob: [_getData] err 404 create new database');
              resolve({ list: [], secret: '' });
              return;
          }

          const downloaded = await streamToBuffer(downloadBlockBlobResponse.readableStreamBody);
          const data = JSON.parse(downloaded.toString());
          this.logger.trace({ downloaded }, 'blob: [_getData] get data @{body}');
          resolve(data);
          return;
        } catch (e) {
          reject(e);
        }
      });
    } else {
      this.logger.trace('blob: [_getData] already exist');
    }

    return this._localData as LocalStorage;
  }

  public saveToken(token: Token): Promise<void> {
    this.logger.warn({ token }, 'save token has not been implemented yet @{token}');

    return Promise.reject(getServiceUnavailable('[saveToken] method not implemented'));
  }

  public deleteToken(user: string, tokenKey: string): Promise<void> {
    this.logger.warn({ tokenKey, user }, 'delete token has not been implemented yet @{user}');

    return Promise.reject(getServiceUnavailable('[deleteToken] method not implemented'));
  }

  public readTokens(filter: TokenFilter): Promise<Token[]> {
    this.logger.warn({ filter }, 'read tokens has not been implemented yet @{filter}');

    return Promise.reject(getServiceUnavailable('[readTokens] method not implemented'));
  }
}
