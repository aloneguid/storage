# Key-Value Storage Providers

This page lists key-value storage providers available in Storage.Net

## Index

- [CSV Files](#icsv-files)
- [Azure Table Storage](#azure-table-storage)

### CSV Files

CSV files implementation maps a local directory to `IKeyValueStorage` and data will be stored in CSV files in that directory. 

For each table a new subfolder will be created called *tableName*.partition and inside the folder you will have files for each partition key named *partitionName*.partition.csv.

To construct, use:

```csharp
IKeyValueStorage storage = StorageFactory.KeyValue.CsvFiles(rootDirectory);
```

To create from connection string:

```csharp
IKeyValueStorage storage = StorageFactory.KeyValue.FromConnectionString("disk://path=rootDirectoryPath");
```

### Azure Table Storage

In order to use Microsoft Azure Table storage you need to reference [![NuGet](https://img.shields.io/nuget/v/Storage.Net.Microsoft.Azure.Storage.svg)](https://www.nuget.org/packages/Storage.Net.Microsoft.Azure.Storage/) package first. The provider wraps around the standard Microsoft Storage SDK.

To construct, use:

```csharp
IKeyValueStorage storage = StorageFactory.KeyValue.AzureTableStorage("account_name", "account_key");
```

Create using connection string:

```csharp
IKeyValueStorage storage = StorageFactory.KeyValue.FromConnectionString("azure.tables://account=account_name;key=storage_key");
```

#### Using Azure Storage Emulator

Either use the factory method:

```csharp
IKeyValueStorage storage = StorageFactory.KeyValue.AzureTableDevelopmentStorage();
```

or use the special connection string:

```csharp
IKeyValueStorage storage = StorageFactory.KeyValue.FromConnectionString("azure.tables://development=true");
```