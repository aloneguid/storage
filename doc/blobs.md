# Blob Storage Providers

This page lists blob storage providers available in Storage.Net

## Index

- [In-Memory](#inmemory)
- [Local Disk](#local-disk)
- [Zip File](#zip-file)

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

