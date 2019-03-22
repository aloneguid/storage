# Key-Value Providers

This page lists key-value storage providers available in Storage.Net

## Index

- [In-Memory](#inmemory)
- [Local Disk](#local-disk)
- [Zip File](#zip-file)
- [FTP](#ftp)
- [Microsoft Azure Blob Storage](#microsoft-azure-blob-storage)

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

The provider is built into Storage.Net main package as zip API are a part of .NET Standard nowadays.

```csharp
IBlobStorage storage = StorageFactory.Blobs.ZipFile(pathToZipFile);
```

To create from connection string:

```csharp
IBlobStorage storage = StorageFactory.Blobs.FromConnectionString("zip://path=path_to_file");
```

...

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

This package treats the first part of the path as **container name**. This allows you to have access to all the containers at once. For instance, path `root/file.txt` creates file `file.txt` in the root of container called `root`. `root/folder1/file.txt` creates file `file.txt` in folder `folder1` under container `root` and so on.





