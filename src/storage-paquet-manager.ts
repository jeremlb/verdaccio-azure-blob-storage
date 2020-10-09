import { S3, AWSError } from 'aws-sdk';
import { UploadTarball, ReadTarball } from '@verdaccio/streams';
import { HEADERS, HTTP_STATUS, VerdaccioError } from '@verdaccio/commons-api';
import { Callback, Logger, Package, ILocalPackageManager, CallbackAction, ReadPackageCallback } from '@verdaccio/types';
import { HttpError } from 'http-errors';

import { is404Error, convertS3Error, create409Error } from './s3Errors';
import { deleteKeyPrefix } from './deleteKeyPrefix';
import { StorageConfig } from './config';
import { addTrailingSlash, streamToBuffer } from './utils';
import { BlobServiceClient, ContainerClient, StorageSharedKeyCredential } from '@azure/storage-blob';

const pkgFileName = 'package.json';

export default class StoragePackageManager implements ILocalPackageManager {
  public config: StorageConfig;
  public logger: Logger;
  private readonly packageName: string;
  private readonly container: ContainerClient;
  private readonly packagePath: string;

  public constructor(config: StorageConfig, packageName: string, logger: Logger) {
    this.config = config;
    this.packageName = packageName;
    this.logger = logger;
    const { accountName, accountKey, endpointSuffix, keyPrefix, containerName } = config;

    const creds = new StorageSharedKeyCredential(accountName, accountKey);
    const blob = new BlobServiceClient(`https://${accountName}.blob.${endpointSuffix}`, creds);

    this.container = blob.getContainerClient(containerName);

    this.logger.trace({ packageName }, 'blob: [StoragePackageManager constructor] packageName @{packageName}');
    this.logger.trace({ accountName }, 'blob: [StoragePackageManager constructor] accountName @{accountName}');
    this.logger.trace({ accountKey }, 'blob: [StoragePackageManager constructor] accountKey @{accountKey}');
    this.logger.trace({ endpointSuffix }, 'blob: [StoragePackageManager constructor] endpointSuffix @{endpointSuffix}');
    this.logger.trace({ keyPrefix }, 'blob: [StoragePackageManager constructor] keyPrefix @{keyPrefix}');
    this.logger.trace({ containerName }, 'blob: [StoragePackageManager constructor] containerName @{containerName}');

    const packageAccess = this.config.getMatchedPackagesSpec(packageName);
    if (packageAccess) {
      const storage = packageAccess.storage;
      const packageCustomFolder = addTrailingSlash(storage);
      this.packagePath = `${this.config.keyPrefix}${packageCustomFolder}${this.packageName}`;
    } else {
      this.packagePath = `${this.config.keyPrefix}${this.packageName}`;
    }
  }

  public updatePackage(
    name: string,
    updateHandler: Callback,
    onWrite: Callback,
    transformPackage: Function,
    onEnd: Callback
  ): void {
    this.logger.debug({ name }, 'blob: [StoragePackageManager updatePackage init] @{name}');
    (async (): Promise<any> => {
      try {
        const json = await this._getData();
        updateHandler(json, err => {
          if (err) {
            this.logger.error({ err }, 'blob: [StoragePackageManager updatePackage updateHandler onEnd] @{err}');
            onEnd(err);
          } else {
            const transformedPackage = transformPackage(json);
            this.logger.debug(
              { transformedPackage },
              'blob: [StoragePackageManager updatePackage updateHandler onWrite] @{transformedPackage}'
            );
            onWrite(name, transformedPackage, onEnd);
          }
        });
      } catch (err) {
        this.logger.error({ err }, 'blob: [StoragePackageManager updatePackage updateHandler onEnd catch] @{err}');

        return onEnd(err);
      }
    })();
  }


  private async _getData(): Promise<unknown> {
    this.logger.debug('blob: [StoragePackageManager _getData init]');

    return await new Promise(async (resolve, reject): Promise<void> => {
      const blob = this.container.getBlobClient(`${this.packagePath}/${pkgFileName}`);
      const client = blob.getBlockBlobClient();

      try {
        const downloadBlockBlobResponse = await client.download();
        const downloaded = await streamToBuffer(downloadBlockBlobResponse.readableStreamBody);
        const data = JSON.parse(downloaded.toString());
        this.logger.trace({ body: downloaded.toString() }, 'blob: [StoragePackageManager _getData] get data @{body}');
        resolve(data);
        return;
      } catch (e: Error) {
        this.logger.error({ err: e.message }, 'blob: [StoragePackageManager _getData] storage @{err}');
        reject(e);
      }
    });
  }

  public removePackage(callback: CallbackAction): void {
    deleteKeyPrefix(
      this.s3,
      {
        Bucket: this.config.bucket,
        Prefix: `${this.packagePath}`,
      },
      function(err) {
        if (err && is404Error(err as VerdaccioError)) {
          callback(null);
        } else {
          callback(err);
        }
      }
    );
  }

  public deletePackage(fileName: string, callback: Callback): void {
    const blob = this.container.getBlobClient(`${this.packagePath}/${pkgFileName}`);
    const client = blob.getBlockBlobClient();

    client.delete()
      .then(() => {
        this.logger.trace({}, 'blob: [StoragePackageManager deletePackage] package deleted');
        callback(null);
      })
      .catch((e) => {
        this.logger.trace({ err: e.message }, 'blob: [StoragePackageManager deletePackage] error @{err}');
        callback(e);
      });
  }

  public createPackage(name: string, value: Package, callback: CallbackAction): void {
    this.logger.debug(
      { name, packageName: this.packageName },
      'blob: [StoragePackageManager createPackage init] name @{name}/@{packageName}'
    );
    this.logger.trace({ value }, 'blob: [StoragePackageManager createPackage init] name @value');
    this.s3.headObject(
      {
        Bucket: this.config.bucket,
        Key: `${this.packagePath}/${pkgFileName}`,
      },
      (err, data) => {
        if (err) {
          const s3Err = convertS3Error(err);
          // only allow saving if this file doesn't exist already
          if (is404Error(s3Err)) {
            this.logger.debug({ s3Err }, 'blob: [StoragePackageManager createPackage] 404 package not found]');
            this.savePackage(name, value, callback);
            this.logger.trace({ data }, 'blob: [StoragePackageManager createPackage] package saved data from blob: @data');
          } else {
            this.logger.error({ s3Err: s3Err.message }, 'blob: [StoragePackageManager createPackage error] @s3Err');
            callback(s3Err);
          }
        } else {
          this.logger.debug('blob: [StoragePackageManager createPackage ] package exist already');
          callback(create409Error());
        }
      }
    );
  }

  public savePackage(name: string, value: Package, callback: CallbackAction): void {
    this.logger.debug(
      { name, packageName: this.packageName },
      'blob: [StoragePackageManager savePackage init] name @{name}/@{packageName}'
    );
    this.logger.trace({ value }, 'blob: [StoragePackageManager savePackage ] init value @value');
    this.s3.putObject(
      {
        // TODO: not sure whether save the object with spaces will increase storage size
        Body: JSON.stringify(value, null, '  '),
        Bucket: this.config.bucket,
        Key: `${this.packagePath}/${pkgFileName}`,
      },
      callback
    );
  }

  public readPackage(name: string, callback: ReadPackageCallback): void {
    this.logger.debug(
      { name, packageName: this.packageName },
      'blob: [StoragePackageManager readPackage init] name @{name}/@{packageName}'
    );
    (async (): Promise<void> => {
      try {
        const data: Package = (await this._getData()) as Package;
        this.logger.trace(
          { data, packageName: this.packageName },
          'blob: [StoragePackageManager readPackage] packageName: @{packageName} / data @data'
        );
        callback(null, data);
      } catch (err) {
        this.logger.error({ err: err.message }, 'blob: [StoragePackageManager readPackage] @{err}');

        callback(err);
      }
    })();
  }

  public writeTarball(name: string): UploadTarball {
    this.logger.debug(
      { name, packageName: this.packageName },
      'blob: [StoragePackageManager writeTarball init] name @{name}/@{packageName}'
    );
    const uploadStream = new UploadTarball({});

    let streamEnded = 0;
    uploadStream.on('end', () => {
      this.logger.debug(
        { name, packageName: this.packageName },
        'blob: [StoragePackageManager writeTarball event: end] name @{name}/@{packageName}'
      );
      streamEnded = 1;
    });

    const baseS3Params = {
      Bucket: this.config.bucket,
      Key: `${this.packagePath}/${name}`,
    };

    // NOTE: I'm using listObjectVersions so I don't have to download the full object with getObject.
    // Preferably, I'd use getObjectMetadata or getDetails when it's available in the node sdk
    // TODO: convert to headObject
    this.s3.headObject(
      {
        Bucket: this.config.bucket,
        Key: `${this.packagePath}/${name}`,
      },
      err => {
        if (err) {
          const convertedErr = convertS3Error(err);
          this.logger.error({ error: convertedErr.message }, 'blob: [StoragePackageManager writeTarball headObject] @{error}');

          if (is404Error(convertedErr) === false) {
            this.logger.error(
              {
                error: convertedErr.message,
              },
              'blob: [StoragePackageManager writeTarball headObject] non a 404 emit error: @{error}'
            );

            uploadStream.emit('error', convertedErr);
          } else {
            this.logger.debug('blob: [StoragePackageManager writeTarball managedUpload] init stream');
            const managedUpload = this.s3.upload(Object.assign({}, baseS3Params, { Body: uploadStream }));
            // NOTE: there's a managedUpload.promise, but it doesn't seem to work
            const promise = new Promise((resolve): void => {
              this.logger.debug('blob: [StoragePackageManager writeTarball managedUpload] send');
              managedUpload.send((err, data) => {
                if (err) {
                  const error: HttpError = convertS3Error(err);
                  this.logger.error(
                    { error: error.message },
                    'blob: [StoragePackageManager writeTarball managedUpload send] emit error @{error}'
                  );

                  uploadStream.emit('error', error);
                } else {
                  this.logger.trace(
                    { data },
                    'blob: [StoragePackageManager writeTarball managedUpload send] response @{data}'
                  );

                  resolve();
                }
              });

              this.logger.debug({ name }, 'blob: [StoragePackageManager writeTarball uploadStream] emit open @{name}');
              uploadStream.emit('open');
            });

            uploadStream.done = (): void => {
              const onEnd = async (): Promise<void> => {
                try {
                  await promise;

                  this.logger.debug('blob: [StoragePackageManager writeTarball uploadStream done] emit success');
                  uploadStream.emit('success');
                } catch (err) {
                  // already emitted in the promise above, necessary because of some issues
                  // with promises in jest
                  this.logger.error({ err }, 'blob: [StoragePackageManager writeTarball uploadStream done] error @{err}');
                }
              };
              if (streamEnded) {
                this.logger.trace(
                  { name },
                  'blob: [StoragePackageManager writeTarball uploadStream] streamEnded true @{name}'
                );
                onEnd();
              } else {
                this.logger.trace(
                  { name },
                  'blob: [StoragePackageManager writeTarball uploadStream] streamEnded false emit end @{name}'
                );
                uploadStream.on('end', onEnd);
              }
            };

            uploadStream.abort = (): void => {
              this.logger.debug('blob: [StoragePackageManager writeTarball uploadStream abort] init');
              try {
                this.logger.debug('blob: [StoragePackageManager writeTarball managedUpload abort]');
                managedUpload.abort();
              } catch (err) {
                const error: HttpError = convertS3Error(err);
                uploadStream.emit('error', error);

                this.logger.error(
                  { error },
                  'blob: [StoragePackageManager writeTarball uploadStream error] emit error @{error}'
                );
              } finally {
                this.logger.debug(
                  { name, baseS3Params },
                  'blob: [StoragePackageManager writeTarball uploadStream abort] s3.deleteObject @{name}/@baseS3Params'
                );

                this.s3.deleteObject(baseS3Params);
              }
            };
          }
        } else {
          this.logger.debug({ name }, 'blob: [StoragePackageManager writeTarball headObject] emit error @{name} 409');

          uploadStream.emit('error', create409Error());
        }
      }
    );

    return uploadStream;
  }

  public readTarball(name: string): ReadTarball {
    this.logger.debug(
      { name, packageName: this.packageName },
      'blob: [StoragePackageManager readTarball init] name @{name}/@{packageName}'
    );
    const readTarballStream = new ReadTarball({});

    const request = this.s3.getObject({
      Bucket: this.config.bucket,
      Key: `${this.packagePath}/${name}`,
    });

    let headersSent = false;

    const readStream = request
      .on('httpHeaders', (statusCode, headers) => {
        // don't process status code errors here, we'll do that in readStream.on('error'
        // otherwise they'll be processed twice

        // verdaccio force garbage collects a stream on 404, so we can't emit more
        // than one error or it'll fail
        // https://github.com/verdaccio/verdaccio/blob/c1bc261/src/lib/storage.js#L178
        this.logger.debug(
          { name, packageName: this.packageName },
          'blob: [StoragePackageManager readTarball httpHeaders] name @{name}/@{packageName}'
        );
        this.logger.trace({ headers }, 'blob: [StoragePackageManager readTarball httpHeaders event] headers @headers');
        this.logger.trace(
          { statusCode },
          'blob: [StoragePackageManager readTarball httpHeaders event] statusCode @statusCode'
        );
        if (statusCode !== HTTP_STATUS.NOT_FOUND) {
          if (headers[HEADERS.CONTENT_LENGTH]) {
            const contentLength = parseInt(headers[HEADERS.CONTENT_LENGTH], 10);

            // not sure this is necessary
            if (headersSent) {
              return;
            }

            headersSent = true;

            this.logger.debug('blob: [StoragePackageManager readTarball readTarballStream event] emit content-length');
            readTarballStream.emit(HEADERS.CONTENT_LENGTH, contentLength);
            // we know there's content, so open the stream
            readTarballStream.emit('open');
            this.logger.debug('blob: [StoragePackageManager readTarball readTarballStream event] emit open');
          }
        } else {
          this.logger.trace('blob: [StoragePackageManager readTarball httpHeaders event] not found, avoid emit open file');
        }
      })
      .createReadStream();

    readStream.on('error', err => {
      const error: HttpError = convertS3Error(err as AWSError);

      readTarballStream.emit('error', error);
      this.logger.error(
        { error: error.message },
        'blob: [StoragePackageManager readTarball readTarballStream event] error @{error}'
      );
    });

    this.logger.trace('blob: [StoragePackageManager readTarball readTarballStream event] pipe');
    readStream.pipe(readTarballStream);

    readTarballStream.abort = (): void => {
      this.logger.debug('blob: [StoragePackageManager readTarball readTarballStream event] request abort');
      request.abort();
      this.logger.debug('blob: [StoragePackageManager readTarball readTarballStream event] request destroy');
      readStream.destroy();
    };

    return readTarballStream;
  }
}
