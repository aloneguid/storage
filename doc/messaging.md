# Messaging Providers

This page lists messaging providers available in Storage.Net

## Index

- [In-Memory](#inmemory)
- [Local Disk](#local-disk)

### In-Memory

In-memory provider creates messaging queue directly in memory, and is useful for mocking message publisher and receiver. It's not intended to be used in production.

The provider is built into Storage.Net main package.

To construct, use:

```csharp
IMessagePublisher publisher = StorageFactory.Messages.InMemoryPublisher(name);

IMessageReceiver receiver = StorageFactory.Messages.InMemoryReceiver(name);
```

`name` in this case is a string that indicates an instance of the queue. Same name points to the same queue, therefore in order to receive messages from a queue with a name, you need to send messages to the queue with the same name.

To construct from a connection string, use:

```csharp
IMessagePublisher publisher = StorageFactory.Messages.PublisherFromConnectionString("inmemory://name=the_name");

IMessageReceiver receiver = StorageFactory.Messages.ReceiverFromConnectionString("inmemory://name=the_name");
```

### Local Disk

Local disk messaging is backed by a local folder on disk. Every message publish call creates a new file in that folder with `.snm` extension (**S**torage **N**et **M**essage) which is a binary representation of the message.

Message receiver polls this folder every second to check for new files, get the oldest ones and transforms into `QueueMessage`. 

The provider is built into Storage.Net main package.

To construct, use:

```csharp
IMessagePublisher publisher = StorageFactory.Messages.DirectoryFilesPublisher(path);

IMessageReceiver receiver = StorageFactory.Messages.DirectoryFilesReceiver(path);
```

`path` is the path to the storage directory. It doesn't have to exist at the moment of construction, and will be created automagically.

To construct from a connection string, use:

```csharp
IMessagePublisher publisher = StorageFactory.Messages.PublisherFromConnectionString("disk://path=the_path");

IMessageReceiver receiver = StorageFactory.Messages.ReceiverFromConnectionString("disk://path=the_path");
```

...