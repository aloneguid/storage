do not edit this, to be deleted

# Microsoft Service Fabric

Microsoft Service Fabric implementations reside in a separate package hosted on [NuGet](https://www.nuget.org/packages/Storage.Net.Microsoft.ServiceFabric/). Follow the link for installation instructions. The package implements blobs and queues.

## Blobs

In Storage.Net blobs map to Azure Service Fabric [Reliable Dictionaries](https://docs.microsoft.com/en-us/dotnet/api/microsoft.servicefabric.data.collections.ireliabledictionary-2?redirectedfrom=MSDN&view=azure-dotnet#microsoft_servicefabric_data_collections_ireliabledictionary_2) where **TKey** is a `string` and **TValue** is a `byte[]`. This mapping allows for maximum flexibility. Having said that, although Storage.Net promotes the use of streams, ASF blob implementation, and ASF collections in general are not designed to store large streams in a single key, but rather a huge collection of smaller streams.

To initialise a reliable dictionary you would normally write the following code:

```csharp
IBlobStorage = StorageFactory.Blobs.AzureServiceFabricReliableStorage(this.StateManager, "collection_name");
``` 

where `collection_name` maps to the name of the collection when obtained via `IReliableStateManager.GetOrAddAsync()`. 

### Transactions

Due to the fact single writes in ASF are not particulary effective, normally when using raw API you would create a transaction, perform writes and then commit it. It is also possible with Storage.Net:

```csharp
using (ITransaction tx = await _blobs.OpenTransactionAsync())
{
    await _blobs.WriteTextAsync("three", "test text 1");
    await _blobs.WriteTextAsync("four", "test text 2");

    await tx.CommitAsync();
}
```

Note that with Storage.Net, unlike raw ASF API you don't have to open a transaction for any method calls. If you do, the calls will use the open transaction object, otherwise a new transaction will be opened and commited for any single call.

## Queues

Queues map to ASF [Reliable Queues](https://docs.microsoft.com/en-us/dotnet/api/microsoft.servicefabric.data.collections.ireliablequeue-1?redirectedfrom=MSDN&view=azure-dotnet#microsoft_servicefabric_data_collections_ireliablequeue_1). Note that [Reliable Concurrent Queue](https://docs.microsoft.com/en-us/azure/service-fabric/service-fabric-reliable-services-reliable-concurrent-queue) is not supported yet, however you can always contribute to the source code if you feel you need it.

The code below show how to create a message publisher and receiver:

```csharp
IMessagePublisher publisher = StorageFactory.Messages.AzureServiceFabricReliableQueuePublisher(this.StateManager, "queue_name");
IMessageReceiver receiver = StorageFactory.Messages.AzureServiceFabricReliableQueueReceiver(this.StateManager, "queue_name");

```

# Appendix 1. Debugging and Contributing

Service Fabric library needs to run inside the cluster in order to debug it, therefore there are a few tricks made in order to make this debuggable, testable and stable.

## Running

There is another solution in `src/service-fabric` folder containing a simple service fabric application with one stateful service. This is to run and debug service fabric storage implementation.

This solution references `Storage.Net` core library and `Storage.Net.Microsoft.ServiceFabric` from the main source folder which allows to debug and edit the code in place while running on a local cluster. This approach is awesome because Service Fabric local clusters and not emulators so we are running as close to the metal as possible.