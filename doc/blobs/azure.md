# Microsoft Azure Blob and File Storage

In order to use Microsoft Azure blob or file storage you need to reference [![NuGet](https://img.shields.io/nuget/v/Storage.Net.Microsoft.Azure.Storage.svg)](https://www.nuget.org/packages/Storage.Net.Microsoft.Azure.Storage/) package first. The provider wraps around the standard Microsoft Storage SDK.

For Storage.Net v9 use the according specific package Blobs [![NuGet](https://img.shields.io/nuget/v/Storage.Net.Microsoft.Azure.Storage.Blobs.svg)](https://www.nuget.org/packages/Storage.Net.Microsoft.Azure.Storage.Blobs/) wich is available for v9.

## File Shares

To create file share by storage name and key:

```csharp
IBlobStorage storage = StorageFactory.Blobs.AzureFiles(accountName, accountKey);
```

or connection string:

```csharp
IBlobStorage storage = StorageFactory.Blobs.FromConnectionString("azure.file://account=account_name;key=secret_value");
```

## Blob Storage

- [Native Operations](#native-operations)
  - [Shared Access Signature Tokens](#sas-tokens)
    - [Storage Account](#storage-account)
    - [Container](#container)
    - [Blob](#blob)
  - [Blob Lease](#blob-lease)

There are a few overloads in this package, for instance:

```csharp
//create from account name and key (secret)
IBlobStorage storage = StorageFactory.Blobs.AzureBlobStorage(accountName, accountKey);

//create to use local development storage emulator
IBlobStorage storage = StorageFactory.Blobs.AzureBlobDevelopmentStorage();

//create an instance of Microsoft Azure Blob Storage that wraps around native CloudBlobClient
IBlobStorage storage = StorageFactory.Blobs.AzureBlobStorage(client);
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


### Native Operations

You can access some native, blob storage specific operations by casting (unsafe) `IBlobStorage` to `IAzureBlobStorage`.

#### SAS Tokens

You can obtain a SAS (Shared Access Signature) tokens to the following objects:

##### Storage Account

Getting SAS token for an account involves granting limited access to entire account. To grant it, for instance, for one hour from now, create a policy first:

```csharp
var policy = new AccountSasPolicy(DateTime.UtcNow, TimeSpan.FromHours(1));
```

By default the policy is configured to give only `List` and `Read` permissions, meaning that users will be able to list containers and blobs, and also read them. You can customise policy permissions by modifying the `Permissions` flag property, for instance to also have `Write` permission you could explicitly assign it:

```csharp
policy.Permissions =
   AccountSasPermission.List |
   AccountSasPermission.Read |
   AccountSasPermission.Write;
```

Then get the policy signature:

```csharp
string sasUrl = await _native.GetStorageSasAsync(policy, true);
```

The second boolean parameter indicates whether to return full URL to the storage with SAS policy or only the policy itself. Setting it to `true` is useful if you want to use this URL in say `Azure Storage Explorer` to attach that account directly. Also, in order to connect to blob storage with SAS, you need the full URL:

To connect to an account using a policy, use the following factory method:

```csharp
IBlobStorage sasInstance = StorageFactory.Blobs.AzureBlobStorageFromSas(sasUrl);
```

##### Container

You can get container's *Shared Access Signature* in the same way as account's one, by calling to

```csharp
string sasUrl = await _native.GetContainerSasAsync(containerName, policy, true);
```

This returns SAS URL that can be used in Azure Storage Explorer, or you can use it to connect in this library itself:

```csharp
IBlobStorage sasInstance = StorageFactory.Blobs.AzureBlobStorageFromSas(sasUrl);
```

Note that the method's signature is identical to account's one, actually it's the same method. Storage.Net takes care of figuring out whether SAS URL is for a container or for a storage account automatically. However, in case of a container SAS, the root folder in `IBlobStorage` instance is the container itself.

##### Blob

In order to get a signature for a specific *blob*, you can use use `GetBlobSasAsync` method. Calling it without any parameters for a blob, returns a *read-only URL valid for 1 hour from now*:

```csharp
string publicUrl = await _native.GetBlobSasAsync(path);
```

You can then redistribute this URL amongst other users so they can download the content.

To customise the policy, pass additional parameters. For instance, to grant *read/write access for 12 hours* you can write the following code:

```csharp
var policy = new BlobSasPolicy(TimeSpan.FromHours(12))
{
   Permissions = BlobSasPermission.Read | BlobSasPermission.Write
};

string publicUrl = await _native.GetBlobSasAsync(path, policy);
```


#### Blob Lease

There is a helper utility method to acquire a block blob lease or a container lease, which is useful for virtual transactions support. For instance:


```csharp
using(AzureStorageLease lease = await _blobs.AcquireLeaseAsync(id, timeSpan))
{
   // your code
}
```

Where the first parameter is blob id or container name, and the second is lease duration. The `BlobLease` returned implements `IDisposable` pattern so that on exit the lease is returned. Note that if blob doesn't exist, current implementation will create a zero-size file and then acquire a least, just for your convenience. The blob is not deleted automatically though.

`AcquireLeaseAsync` also has an option to wait for the lease to be returned (third optional argument) which when set to true causes this library to try to acquire a lease every second until it's released, and re-lease it.

It also exposes `RenewLeaseAsync()` method to renew the lease explicitly.
