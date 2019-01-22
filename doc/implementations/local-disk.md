# Local Disk

Local disk implementation is baked into the Storage.Net library itself as local I/O is a part of framework core.

## Blobs

### Disk

You can map a local directory as `IBlobStorage`

```csharp
IBlobStorage storage = StorageFactory.Blobs.DirectoryFiles(TestDir);
```

which simply stores them as local files. Subfolders are created on demand, as soon as you start introducing path separators into blob IDs.

alternatively, you can create it with a connection string:

```csharp
IBlobStorage storage = StorageFactory.Blobs.FromConnectionString("disk://path=path_to_directory");
```

### In-Memory

Simply stores blobs in process memory. Absolutely inefficient, however may be useful for testing.

```csharp
IBlobStorage storage = StorageFactory.Blobs.InMemory();
```

or

```csharp
IBlobStorage storage = StorageFactory.Blobs.FromConnectionString("inmemory://");
```

## Tables

As for tables, you can map a local directory to `ITableStorage`

```csharp
ITableStorage tables = StorageFactory.Tables.CsvFiles(TestDir);
```

and data will be stored in CSV files in that directory. 

For each table a new subfolder will be created called *tableName*.partition and inside the folder you will have files for each partition key named *partitionName*.partition.csv.

## Messaging

### In-Memory

In-memory messaging simply caches message in an in-memory queues. Create a publisher:

```csharp
IMessagePublisher publisher = StorageFactory.Messages.InMemoryPublisher("buffer_name");
```

or

```csharp
IMessagePublisher publisher = StorageFactory.Messages.PublisherFromConnectionString("inmemory://name=buffer_name");
```

**buffer_name** is a name of memory buffer where messages get published or received from and it serves a way to create more than publisher/receiver pair by giving them different names.

To create a receiver:


```csharp
IMessageReceiver receiver = StorageFactory.Messages.InMemoryReceiver("buffer_name");
```

or

```csharp
IMessagePublisher publisher = StorageFactory.Messages.ReceiverFromConnectionString("inmemory://name=buffer_name");
```