# Blob Storage Providers

This page lists blob storage providers available in Storage.Net

## Index

- [In-Memory](#inmemory)
- [Local Disk](#local-disk)
- [Zip File](#zip-file)
- [FTP](#ftp)
- [Microsoft Azure Blob Storage](#microsoft-azure-blob-storage)
- [Amazon S3 Storage](#amazon-s3-storage)
- [Azure Data Lake Store](#azure-data-lake-store)
  - [Gen 1](#gen-1) 
  - [Gen 2](#gen-2) 

### In-Memory

In-memory provider stores blobs in process memory as array of bytes. Although it's available, we strongly discourage using it in production due to high memory fragmentation. It's sole purpose is for local testing, mocking etc.

The provider is built into Storage.Net main package.

To construct, use:

```csharp
IBlobStorage storage = StorageFactory.Blobs.InMemory();
```

which constructs a new instance of in-memory storage. Further calls to this factory create a new unique instance.

To create from connection string:

```csharp
IBlobStorage storage = StorageFactory.Blobs.FromConnectionString("inmemory://");
```

### Local Disk

Local disk providers maps a local folder to `IBlobStorage` instance, so that you can both use local disk and replicate directory structure as blob storage interface.

The provider is built into Storage.Net main package.

```csharp
IBlobStorage storage = StorageFactory.Blobs.DirectoryFiles(directory);
```

where `directory` is an instance of `System.IO.DirectoryInfo` that points to that directory. The directory does not have to exist on local disk, however it will be created on any write operation.

To create from connection string:

```csharp
IBlobStorage storage = StorageFactory.Blobs.FromConnectionString("disk://path=path_to_directory");
```

### Zip File

Zip file provider maps to a single zip archive. All the operations that include path are created as subfolders in zip archive. Archive itself doesn't need to exist, however any write operation will create a new archive and put any data you write to it.

The provider is built into Storage.Net main package as zip API are a part of .NET Standard nowadays. It is thread safe by default.

```csharp
IBlobStorage storage = StorageFactory.Blobs.ZipFile(pathToZipFile);
```

To create from connection string:

```csharp
IBlobStorage storage = StorageFactory.Blobs.FromConnectionString("zip://path=path_to_file");
```

### FTP

FTP implementation is wrapping an amazing [FluentFTP](https://github.com/robinrodricks/FluentFTP) library. As this is an external library, you need to reference [![NuGet](https://img.shields.io/nuget/v/Storage.Net.Ftp.svg)](https://www.nuget.org/packages/Storage.Net.Ftp/) package first.

The provider respects folder structure of the remote FTP share.

You can instantiate it either by using a simple helper method accepting the most basic parameters, like hostname, username and password, however for custom scenarios you can always construct your own instance of `FtpClient` from the FluentFTP library and pass it to Storage.Net to manage:

```csharp
IBlobStorage storage = StorageFactory.Blobs.Ftp("myhost.com", new NetworkCredential("username", "password"));

// specify a custom ftp port (12345) as an example, real world scenarios may need extra customisations
var client = new FtpClient("myhost.com", 12345, new NetworkCredential("username", "password"));
IBlobStorage storage = StorageFactory.Blobs.FtpFromFluentFtpClient(client);
```

To create from connection string, first register the module when your program starts by calling `StorageFactory.Modules.UseFtpStorage();` then use the following connections tring:

```csharp
IBlobStorage storage = StorageFactory.Blobs.FromConnectionString("ftp://host=hostname;user=username;password=password");
```


### Microsoft Azure Blob Storage

In order to use Microsoft Azure blob storage you need to reference [![NuGet](https://img.shields.io/nuget/v/Storage.Net.Microsoft.Azure.Storage.svg)](https://www.nuget.org/packages/Storage.Net.Microsoft.Azure.Storage/) package first. The provider wraps around the standard Microsoft Storage SDK.

There are a few overloads in this package, for instance:

```csharp
//create from account name and key (secret)
IBlobStorage storage = StorageFactory.Blobs.AzureBlobStorage(accountName, accountKey);

//create to use local development storage emulator
IBlobStorage storage = StorageFactory.Blobs.AzureBlobDevelopmentStorage();

//create an instance of Microsoft Azure Blob Storage that wraps around native CloudBlobClient
IBlobStorage storage = StorageFactory.Blobs.AzureBloStorage(client);
```

Please use the native option with caution, as it exposes the internal native client reference which may change in future.

To use connection strings, first register the module when your program starts by calling `StorageFactory.Modules.UseAzureStorage();` then use the following:

```csharp
//using account name and key
IBlobStorage storage = StorageFactory.Blobs.FromConnectionString("azure.blob://account=account_name;key=secret_value");

//local development emulator
IBlobStorage storage = StorageFactory.Blobs.FromConnectionString("azure.blob://development=true");
```

This storage is working with `block blobs` only. We are planning to add `append blobs` support but that requires some architectural changes and as always you're welcome to help.

This package treats the first part of the path as **container name**. This allows you to have access to all the containers at once. For instance, path `root/file.txt` creates file `file.txt` in the root of container called `root`. `root/folder1/file.txt` creates file `file.txt` in folder `folder1` under container `root` and so on. You can check if the folder returned is a container by referring to `isContainer` custom property (`blob.Properties["IsContainer"] == "True"`).


#### Native Operations

You can access some native, blob storage specific operations by casting (unsafe) `IBlobStorage` to `IAzureBlobStorage`.

##### SAS Tokens

Please see the interface details or contribute to the docs ;)

##### Blob Lease (aka Lock)

There is a helper utility method to acquire a block blob lease, which is useful for virtual transactions support. For instance:


```csharp
using(BlobLease lease = await _blobs.AcquireBlobLeaseAsync(id, timeSpan))
{
   // your code
}
```

Where the first parameter is blob id and the second is lease duration. The `BlobLease` returned implements `IDisposable` pattern so that on exit the lease is returned. Note that if blob doesn't exist, current implementation will create a zero-size file and then acquire a least, just for your convenience. The blob is not deleted automatically though.

`AcquireBlobLeaseAsync` also has an option to wait for the lease to be returned (third optional argument) which when set to true causes this library to try to acquire a lease every second until it's released, and re-lease it.

It also exposes `RenewLeaseAsync()` method to renew the lease explicitly, and `LeasedBlob` property that returns a native `CloudBlockBlob` that is leased if you need to explicitly call any methods not supported by this wrapper.

### Amazon S3 Storage

In order to use Microsoft Azure blob storage you need to reference [![NuGet](https://img.shields.io/nuget/v/Storage.Net.Amazon.Aws.svg)](https://www.nuget.org/packages/Storage.Net.Amazon.Aws/) package first. The provider wraps around the standard AWS SDK which is updated regularry.

There are a few overloads in this package, for instance:

```csharp
IBlobStorage storage = StorageFactory.Blobs.AmazonS3BlobStorage(string accessKeyId,
   string secretAccessKey,
   string bucketName,
   RegionEndpoint regionEndpoint = null);
```

Please see `StorageFactory.Blobs` factory entry for more options.

To create with a connection string, first reference the module:

```csharp
StorageFactory.Modules.UseAwsStorage();
```

Then construct using the following format:

```csharp
IBlobStorage storage = StorageFactory.Blobs.FromConnectionString("aws.s3://keyId=...;key=...;bucket=...;region=...");
```

where:
- **keyId** is (optional) access key ID.
- **key** is (optional) secret access key.
- **bucket** is bucket name.
- **region** is an optional value and defaults to `EU West 1` if not specified. At the moment of this wring the following regions are supported. However as we are using the official AWS SDK, when region information changes, storage.net gets automatically updated.
  - `us-east-1`
  - `us-east-2`
  - `us-west-1`
  - `us-west-2`
  - `eu-north-1`
  - `eu-west-1`
  - `eu-west-2`
  - `eu-west-3`
  - `eu-central-1`
  - `ap-northeast-1`
  - `ap-northeast-2`
  - `ap-northeast-3`
  - `ap-south-1`
  - `ap-southeast-1`
  - `ap-southeast-2`
  - `sa-east-1`
  - `us-gov-east-1`
  - `us-gov-west-1`
  - `cn-north-1`
  - `cn-northwest-1`
  - `ca-central-1`

If **keyId** and **key** are omitted, the AWS SDK's default approach to credential resolution will be used. For example: if running in Lambda, it will assume the Lambda execution role; if there are credentials configured in ~/.aws, it will use those; etc.  See [AWS SDK Documentation](https://docs.aws.amazon.com/sdk-for-net/v3/developer-guide/net-dg-config-creds.html) for more details.

#### Native Operations

Native operations are exposed via [IAwsS3BlobStorageNativeOperations](../src/AWS/Storage.Net.Amazon.Aws/Blobs/IAwsS3BlobStorage.cs) interface.

## Azure Data Lake Store

In order to use, reference [![NuGet](https://img.shields.io/nuget/v/Storage.Net.Microsoft.Azure.DataLake.Store.svg)](https://www.nuget.org/packages/Storage.Net.Microsoft.Azure.DataLake.Store/) package first. Both **Gen1** and **Gen2** storage accounts are supported.

### Gen 1

To create using a factory method, use the following signature:

```csharp
IBlobStorage storage = StorageFactory.Blobs.AzureDataLakeGen1StoreByClientSecret(
         string accountName,
         string tenantId,
         string principalId,
         string principalSecret,
         int listBatchSize = 5000)
```

The last parameter *listBatchSize* indicates how to query storage for list operations - by default a batch of 5k items will be used. Note that the larger the batch size, the more data you will receive in the request. This speeds up list operations, however may result in HTTP time-out the slower your internet connection is. This feature is not available in the standard .NET SDK and was implemented from scratch.

You can also use connection strings:

```csharp
IBlobStorage storage = StorageFactory.Blobs.FromConnectionString("azure.datalake.gen1://account=...;tenantId=...;principalId=...;principalSecret=...;listBatchSize=...");
```

the last parameter *listBatchSize* is optional and defaults to `5000`.

### Gen 2

Gen 2 is the new generation of the storage API, and you should always prefer it to Gen 1 accounts when you can. Both Gen 1 and Gen 2 providers are located in the same NuGet package.

Gen 2 provider is 100% compatible with hierarchical namespaces. When you use blob path, the first part of the path is filesystem name, i.e. `storage.WriteTextAsync("filesystem/folder/subfolder/.../file.extension`. Apparently you cannot create files in the root folder, they always need to be prefixed with filesystem name.

If filesystem doesn't exist, we will try to create it for you, if the account provided has enough permissions to do so.

#### Authentication

You can authenticate in the ways described below. To use connection strings, don't forget to call `StorageFactory.Modules.UseAzureDataLake()` somewhere when your program starts.

##### Using **Shared Key Authentication**

```csharp
IBlobStorage storage = StorageFactory.Blobs.AzureDataLakeGen2StoreBySharedAccessKey(
   accountName,
   sharedKey);
```

or

```csharp
IBlobStorage storage = StorageFactory.Blobs.FromConnectionString(
   "azure.datalake.gen2://account=...;key=...");
```

##### Using **Service Principal**

```csharp
IBlobStorage storage = StorageFactory.Blobs.AzureDataLakeGen2StoreByClientSecret(
   accountName,
   tenantId,
   principalId,
   principalSecret);
```

or

```csharp
IBlobStorage storage = StorageFactory.Blobs.FromConnectionString(
   "azure.datalake.gen2://account=...;tenantId=...;principalId=...;principalSecret=...;listBatchSize=...");
```

##### Using **Managed Service Identity**

```csharp
IBlobStorage storage = StorageFactory.Blobs.AzureDataLakeGen2StoreByManagedIdentity(
   accountName);
```

or

```csharp
IBlobStorage storage = StorageFactory.Blobs.FromConnectionString(
   "azure.datalake.gen2://account=...;msi");
```




