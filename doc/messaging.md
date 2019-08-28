# Messaging Providers

This page lists messaging providers available in Storage.Net

## Index

- [In-Memory](#inmemory)
- [Local Disk](#local-disk)
- [Azure Storage Queue](#azure-storage-queue)
- [Azure Service Bus](#azure-service-bus)
- [Amazon Simple Queue](#amazon-simple-queue)

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

### Azure Storage Queue

```csharp
IMessagePublisher publisher = StorageFactory.Messages.AzureStorageQueuePublisher();

IMessageReceiver receiver = StorageFactory.Messages.AzureStorageQueueReceiver();
```

```csharp
IMessagePublisher publisher = StorageFactory.Messages.PublisherFromConnectionString("azure.queue://account=...;key=...;queue=...");

IMessageReceiver receiver = StorageFactory.Messages.ReceiverFromConnectionString("azure.queue://account=..;key=...;queue=...");
```

### Azure Service Bus

In order to use Microsoft Azure Service Bus you need to reference
[![NuGet](https://img.shields.io/nuget/v/Storage.Net.Microsoft.Azure.ServiceBus.svg)](https://www.nuget.org/packages/Storage.Net.Microsoft.Azure.ServiceBus/) first. The provider wraps around the standard Microsoft Service Bus SDK.

This provider supports both topics and queues, for publishing and receiving. To construct a publisher use the following:

```csharp
IMessagePublisher queuePublisher = StorageFactory.Messages.AzureServiceBusQueuePublisher(
                       connectionString,
                       queueName);

IMessagePublisher topicPublisher = StorageFactory.Messages.AzureServiceBusTopicPublisher(
                       connectionString,
                       topicName);
```

To construct a receiver, use the following:


```csharp
IMessageReceiver queueReceiver = StorageFactory.Messages.AzureServiceBusQueueReceiver(
                       connectionString,
                       queueName,
                       peekLock);

IMessageReceiver topicReceiver = StorageFactory.Messages.AzureServiceBusTopicReceiver(
                       connectionString,
                       topicName,
                       subscriptionName,
                       peekLock);
```

**peekLock** is a flag indicating [how to receive the message](https://docs.microsoft.com/en-us/rest/api/servicebus/peek-lock-message-non-destructive-read), is optional, and is `true` by default.

You also have an options to pass your own [MessageHandlerOptions](https://docs.microsoft.com/en-us/dotnet/api/microsoft.azure.servicebus.messagehandleroptions?view=azure-dotnet) to customise service bus behavior. For instance, if you need to define an exception handler that doesn't ignore errors (default behavior) and set for instance concurrency to more than 1 message (default) you could write the following:

```csharp
var options = new MessageHandlerOptions(tExceptionReceiverHandler)
{
  AutoComplete = false,
  MaxAutoRenewDuration = TimeSpan.FromMinutes(1),
  MaxConcurrentCalls = 5 //instead of 1
};

private async Task ExceptionReceiverHandler(ExceptionReceivedEventArgs args)
{
    // your exception handling code
}

IMessageReceiver topicReceiver = StorageFactory.Messages.AzureServiceBusTopicReceiver(
                       connectionString,
                       topicName,
                       subscriptionName,
                       peekLock,
                       options);
```

Note that exception handler in Service Bus is for *informational purposes only*, it doesn't actually handle exceptions, and in case of errors the SDK [retries them automatically](https://docs.microsoft.com/en-us/dotnet/api/microsoft.azure.servicebus.messagehandleroptions.exceptionreceivedhandler?view=azure-dotnet#Microsoft_Azure_ServiceBus_MessageHandlerOptions_ExceptionReceivedHandler).



### Amazon Simple Queue

In order to use [Amazon Simple Queue](https://aws.amazon.com/sqs/) you need to reference
[![NuGet](https://img.shields.io/nuget/v/Storage.Net.Amazon.Aws.svg)](https://www.nuget.org/packages/Storage.Net.Amazon.Aws/) first. The provider wraps around the standard AWS SDK.

To construct a publisher use the following:

```csharp
IMessagePublisher queuePublisher = StorageFactory.Messages.AmazonSQSMessagePublisher(
  accessKeyId,
  secretAccessKey,
  serviceUrl,
  queueName,
  regionEndpoint);

IMessagePublisher topicPublisher = StorageFactory.Messages.AmazonSQSMessageReceiver(
  accessKeyId,
  secretAccessKey,
  serviceUrl,
  queueName,
  regionEndpoint);
```

- **accessKeyId** and **secretAccessKey*) are credentials to access the queue.
- **serviceUrl** indicates the service URL, for instance `https://sqs.us-east-1.amazonaws.com`
- **queueName** is the name of the queue
- **retionEndpoint** is optional and defaults to `USEast1`

