# Blob Storage Providers

This page lists blob storage providers available in Storage.Net

## Index

- [In-Memory](#inmemory)
- [Local Disk](#local-disk)
- [Zip File](#zip-file)
- [FTP](#ftp)
- [Microsoft Azure Blob and File Storage](blobs/azure.md)
- [Amazon S3 Storage](blobs/awss3.md)
- [Azure Data Lake Store](#azure-data-lake-store)
  - [Gen 1](#gen-1) 
  - [Gen 2](#gen-2)
- [Google Cloud Storage](#google-cloud-storage)
- [Azure Key Vault](#azure-key-vault)
- [Databricks](blobs/databricks.md)

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
   "azure.datalake.gen2://account=...;tenantId=...;principalId=...;principalSecret=...");
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

#### Permissions Management

ADLS Gen 2 [supports](https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-access-control) RBAC and [POSIX](https://www.usenix.org/legacy/publications/library/proceedings/usenix03/tech/freenix03/full_papers/gruenbacher/gruenbacher_html/main.html) like permissions on both file and folder level. Storage.Net fully supports permissions management on those and exposes simplified easy-to-use API to drive them.

Because permission management is ADLS Gen 2 specific feature, you cannot use `IBlobStorage` interface, however you can cast it to `IAzureDataLakeGen2BlobStorage` which in turn implements `IBlobStorage` as well.

In order to get permissions for an object located on a specific path, you can call the API:

```csharp
IBlobStorage genericStorage = StorageFactory.Blobs.AzureDataLakeGen2StoreByClientSecret(name, key);
IAzureDataLakeGen2BlobStorage gen2Storage = (IAzureDataLakeGen2BlobStorage)genericStorage;

//get permissions
AccessControl access = await _storage.GetAccessControlAsync(path);
```

`AccessControl` is a self explanatory structure that contains information about owning user, owning group, their permissions, and any custom ACL entries assigned to this object.

In order to set permissions, you need to call `SetAccessControlAsync` passing back modified `AccessControl` structure. Let's say I'd like to add *write* access to a user with ID `6b157067-78b0-4478-ba7b-ade5c66f1a9a` (Active Directory Object ID). I'd write code like this (using the structure we've just got back from `GetAccessControlAsync`):

```csharp
// add user to custom ACL
access.Acl.Add(new AclEntry(ObjectType.User, userId, false, true, false));

//update the ACL on Gen 2 storage
await _storage.SetAccessControlAsync(path, access);
```

### Google Cloud Storage

In order to use [Google Cloud Storage](https://cloud.google.com/storage/) reference [![NuGet](https://img.shields.io/nuget/v/Storage.Net.Gcp.CloudStorage.svg)](https://www.nuget.org/packages/Storage.Net.Gcp.CloudStorage) package first.

You definitely want to use Storage.Net for working with Google Storage, as it solves quite a few issues which are hard to beat with raw SDK:

- Listing of files and folders
- Recursive and non-recursive listing
- Upload and download operations are continuing to be optimised

You can initialise it in one of few ways:

#### Credentials stored in an environment variable

As described [here](https://cloud.google.com/storage/docs/reference/libraries#setting_up_authentication)

```csharp
IBlobStorage storage = StorageFactory.Blobs.GoogleCloudStorageFromEnvironmentVariable(string bucketName);
```

#### Credentials stored in an external file

```csharp
IBlobStorage storage = StorageFactory.Blobs.GoogleCloudStorageFromEnvironmentVariable(string bucketName, string credentialsFilePath);
```

#### Credentials passed in a string

```csharp
IBlobStorage storage = StorageFactory.Blobs.GoogleCloudStorageFromEnvironmentVariable(
   string bucketName,
   string credentialsJsonString,
   bool isBase64EncodedString = false);
```

This method is fairly interesting, as it allows you to pass credential file content as a string. The last parameter says whether the string is base64 encoded or not, which is handy if credentials are stored in some sort of config file.

#### From connecting string

First, don't forget to initialise the module:

```csharp
StorageFactory.Modules.UseGoogleCloudStorage();
```

Then, use the string:

```csharp
IBlobStorage storage = StorageFactory.Blobs.FromConnectionString("google.storage://bucket=...;cred=...");
```

Where **cred** is a *BASE64* encoded credential string.

### Azure Key Vault

In order to use Key Vault reference [![NuGet](https://img.shields.io/nuget/v/Storage.Net.Microsoft.Azure.KeyVault.svg)](https://www.nuget.org/packages/Storage.Net.Microsoft.Azure.KeyVault) package first.

> todo
