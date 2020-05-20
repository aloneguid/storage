# Storage.Net
### One Interface To Rule Them All
[![NuGet](https://img.shields.io/nuget/v/Storage.Net.svg)](https://www.nuget.org/packages/Storage.Net/) [![](https://img.shields.io/azure-devops/build/aloneguid/4bab84c0-90f7-4425-afc6-ba077aa9757e/49/master.svg)](https://aloneguid.visualstudio.com/AllPublic/_build?definitionId=49) [![open collective backers and sponsors](https://img.shields.io/opencollective/all/storagedotnet.svg)](https://opencollective.com/storagedotnet)

![Slide](doc/slide.svg)

## About

Storage.NET is a field-tested .NET library that helps to achieve [polycloud techniques](https://www.thoughtworks.com/radar/techniques/polycloud). 

It provides generic interface for popular cloud storage providers like Amazon S3, Azure Service Bus, Azure Event Hub, Azure Storage, Azure Data Lake Store thus abstracting Blob and Messaging services.

It also implements in-memory and on-disk versions of all the abstractions for faster local machine development. Connection strings are supported too!

![Comic Why](doc/comic-why.png)

Storage.Net is used by some big **Fortune 500** companies, large, medium and small businesses, open-source projects and even desktop applications like this one:

<a href='//www.microsoft.com/store/apps/9NKV1D43NLL3?cid=storebadge&ocid=badge'>
   <img src='https://assets.windowsphone.com/85864462-9c82-451e-9355-a3d5f874397a/English_get-it-from-MS_InvariantCulture_Default.png' alt='English badge' style='width: 284px; height: 104px;' width='284' height='104'/>
</a>

## Index

- [Intentions](#intentions)
  - [Local Development](#local-development)
- [Implementations](#implementations)
- [Getting Started](#getting-started)
  - [Blob Storage](#blob-storage)
    - [High-level architecture](#high-level-architecture)
    - [Transform Sinks](#transform-sinks)
  - [Messaging](#messaging)
- [Contributing](#contributing)
- [Sponsorship](#sponsorship)

## Intentions

I'm not really sure why there are so many similar storage providers performing almost identical function but no standard. Why do we need to learn a new SDK to achieve something trivial we've done so many times before? I have no idea. If you don't either, use this library.

`Storage.Net` abstracts storage implementation like `blobs`, `tables` and `messages` from the .NET Applicatiion Developer. It's aimed to provide a generic interface regardless on which storage provider you are using. It also provides both synchronous and asynchronous alternatives of all methods and implements it to the best effort possible. 

Storage.Net supports **Azure Service Bus**, **Azure Event Hub**, **Azure Storage**, **Azure Data Lake Store**, **Amazon S3**, **Azure Key Vault** and many more, out of the box, with hassle-free configuration and zero learning path.

### Local Development

Storage.Net also implements inmemory and on disk versions of all the abstractions, therefore you can develop fast on local machine or use vendor free serverless implementations for parts of your applciation which don't require a separate third party backend at a particular point in development.

This framework supports `.NET Standard 2.0` and higher.

## Implementations

![Storagetypes](doc/storagetypes.svg)

Storage.Net defines two different storage types:

- **Blob Storage** is used to store arbitrary files of any size, that do not have any structure. The data is essentially a binary file. Examples of a blog storage is Azure Blob Storage, Amazon S3, local folder etc.
- **Messaging** is an asynchronous mechanism to send and receive messages between disconnected systems. For instance MSMQ, Azure Service Bus, Amazon Simple Queue etc.

Some effort has been made to document the supported storage options, you are welcome to contribute to documentation, or browse the following sections:

- [Blob Storage Implementations](doc/blobs.md)
- [Messaging Implementations](doc/messaging.md)

## Geting Started

### Blob Storage

Blob Storage stores files. A file has only two properties - `ID` and raw data. If you build an analogy with disk filesystem, file ID is a file name.

Blob Storage is really simple abstraction - you read or write file data by it's ID, nothing else.

The entry point to a blog storage is [IBlobStorage](src/Storage.Net/Blobs/IBlobStorage.cs) interface. This interface is small but contains all possible methods to work with blobs, such as uploading and downloading data, listing storage contents, deleting files etc. The interface is kept small so that new storage providers can be added easily, without implementing a plethora of interface methods.

In addition to this interface, there are plency of extension methods which enrich the functionality, therefore you will see more methods than this interface actually declares. They add a lot of useful and functionality rich methods to work with storage. For instance, `IBlobStorage` upload functionality only works with streams, however extension methods allows you to upload text, stream, file or even a class as a blob. Extension methods are also provider agnostic, therefore all the rich functionality just works and doesn't have to be reimplemented in underlying data provider.

All the storage implementations can be created either directly or using factory methods available in the `Storage.Net.StorageFactory.Blobs` class. More methods appear in that class as you reference a **NuGet** package containing specific implementations, however there are a few built-in implementations available out of the box as well. After referencing an appropriate package from NuGet you can call to a storage factory to create a respective storage implementation:

![](doc/storagefactory-intellisense.gif)

You can also use connection strings to create blob storage instances. Connection strings are often useful if you want to completely abstract yourself from the underlying implementation. Please read the appropriate implementation details for connection string details. For instance, to create an instance of Azure Blob Storage provider you could write:

```csharp
IBlobStorage storage = StorageFactory.Blobs.FromConnectionString("azure.blobs://...parameters...");
```

In this example we create a blob storage implementation which happens to be Microsoft Azure blob storage. The project is referencing an appropriate [nuget package](https://www.nuget.org/packages/Storage.Net.Microsoft.Azure.Storage.Blobs/). As blob storage methods promote streaming we create a `MemoryStream` over a string for simplicity sake. In your case the actual stream can come from a variety of sources.

```csharp
using Storage.Net;
using Storage.Net.Blobs;
using System.IO;
using System.Text;

namespace Scenario
{
   public class DocumentationScenarios
   {
      public async Task RunAsync()
      {
         //create the storage using a factory method
         IBlobStorage storage = StorageFactory.Blobs.AzureBlobStorageWithSharedKey(
            "storage name",
            "storage key");

         //upload it
         string content = "test content";
         using (var s = new MemoryStream(Encoding.UTF8.GetBytes(content)))
         {
            await storage.WriteAsync("mycontainer/someid", s);
         }

         //read back
         using (var s = new MemoryStream())
         {
            using (Stream ss = await storage.OpenReadAsync("mycontainer/someid"))
            {
               await ss.CopyToAsync(s);

               //content is now "test content"
               content = Encoding.UTF8.GetString(s.ToArray());
            }
         }
      }
   }
}
```

This is really simple, right? However, the code looks really long and boring. If I need to just save and read a string why the hell do I need to dance around with streams? That was examply my point when trying to use external SDKs. Why do we need to work in an ugly way if all we want to do is something simple? Therefore with Storage.Net you can decrease this code to just two lines of code:

```csharp
public async Task BlobStorage_sample2()
{
    IBlobStorage storage = StorageFactory.Blobs.AzureBlobStorageWithSharedKey(
		"storage name",
		"storage key");

    //upload it
    await storage.WriteTextAsync("mycontainer/someid", "test content");

    //read back
    string content = await storage.ReadTextAsync("mycontainer/someid");
}
```

You can find the list of supported blob storage implementations [here](doc/blobs.md).

#### High-level architecture

The basic architecture of blobs is depicted in the following diagram:

![](doc/blobs-interfaces.svg)

All of the core methods are defined in the `IBlobStorage` interface. This is the interface that's enough for a new storage provider to implement in order to add a new storage provider.

However, some providers support more than just basic operations, for instance Azure Blob Storage supports blob leasing, shared access signatures etc., therefore it actually implements `IAzureBlobStorage` interface that in turn implements `IBlobStorage` interface, and extends the functionality further. Same goes for AWS S3 and others.

However, when you are browsing `IBlobStorage` interface, intellisesnse will shows you a plethora of methods that are not there. This is because there are plenty of **extension methods** defined for it. Extension methods add extra useful stuff, such as ability to write/read strings, JSON objects and so on, but they in turn use only methods from `IBlobStorage`. The decision to split those methods into extension methods was because that is logical functionality not dependent on any underlying implementation. Also implementing new storage providers is much easier, as you only have to implement a subset of methods.

#### Transform Sinks

Transform sinks is another awesome feature of Storage.Net that works across all the storage providers. Transform sinks allow you to **transform data stream** for both upload and download to somehow transform the underlying stream of data. Examples of transform sinks would be *gzipping data* transparently, *encrypting it*, and so on.

Let's say you would like to *gzip* all of the files that you upload/download to a storage. You can do that in the following way:

```csharp
IBlobStorage myGzippedStorage = StorageFactory.Blobs
   .AzureBlobStorageWithSharedKey("name", "key")
   .WithGzipCompression();
```

Then use the storage as you would before - all the data is compressed as you write it (with any `WriteXXX` method) and decompressed as you read it (with any `ReadXXX` method).

For more details on sinks available and how to implement your own sink, [read this page](doc/sinks.md).


##### Implementation Details

Due to the nature of the transforms, they can change both the underlying data, and stream size, therefore there is an issue with storage providers, as they need to know beforehand the size of the blob you are uploading. The matter becomes more complicated when some implementations need to calculate other statistics of the data before uploading i.e. hash, CRC and so on. Therefore the only *reliable* way to stream transformed data is to actually perform all of the transofrms, and then upload it. In this implementation, Storage.Net uses in-memory transforms to achieve this, however does it extremely efficiently by using [Microsoft.IO.RecyclableMemoryStream](https://github.com/Microsoft/Microsoft.IO.RecyclableMemoryStream) package that performs memory pooling and reclaiming for you so that you don't need to worry about software slowdows. You can read more about this technique [here](http://www.philosophicalgeek.com/2015/02/06/announcing-microsoft-io-recycablememorystream/).

This also means that today a transform sink can upload a stream only as large as the amount of RAM available on your machine. I am, however, thinking of ways to go further than that, and there are some beta implementations available that might see the light soon.

![](doc/sinks-memory.svg)


### Messaging

Messaging is inteded for message passing between one or more systems in disconnected fashion. You can send a message somewhere and current or remote system picks it up for processing later when required. This paradigm somehow fits into [CQRS](https://martinfowler.com/bliki/CQRS.html) and [Message Passing](https://www.defit.org/message-passing/) architectural ideas.

To name a few examples, [Apache Kafka](http://kafka.apache.org/), [RabbitMQ](https://www.rabbitmq.com/), [Azure Service Bus](https://azure.microsoft.com/en-gb/services/service-bus/) are all falling into this category - essentially they are designed to pass messages. Some systems are more advanced to others of course, but most often it doesn't really matter.

Storage.Net supports many messaging providers out of the box, including **Azure Service Bus Topics and Queues**, **Azure Event Hub** and others.

There are two abstractions available - **message publisher** and **message receiver**. As the name stands, one is publishing messages, and another is receiving them on another end.

#### Publishing Messages

To publish messages you will usually construct an instance of `IMessagePublisher` with an appropriate implementation. All the available implementations can be created using factory methods in the `Storage.Net.StorageFactory.Messages` class. More methods appear in that class as you reference an assembly containing specific implementations.

#### Receiving Messages

Similarly, to receive messages you can use factory methods to create receivers which all implement `IMessageReceiver` interface.

The primary method of this interface

```csharp
Task StartMessagePumpAsync(
	Func<IEnumerable<QueueMessage>, Task> onMessageAsync,
	int maxBatchSize = 1,
	CancellationToken cancellationToken = default);
```

starts a message pump that listens for incoming queue messages and calls `Func<IEnumerable<QueueMessage>, Task>` as a call back to pass those messages to your code.

`maxBatchSize` is a number specifying how many messages you are ready to handle at once in your callback. Choose this number carefully as specifying number too low will result in slower message processing, whereas number too large will increase RAM requirements for your software.

`cancellationToken` is used to signal the message pump to stop. Not passing any parameter there will result in never stopping message pump. See example below in Use Cases for a pattern on how to use this parameter.

You can find the list of supported messaging implementations [here](doc/messaging.md).

#### Handling Large Messages

Storage.Net provides built-in capability to handle large message content by allowing you to offload message content over a certain threshold to an external blob storage. It works in the following way:

1. Check that message content is larger than `threshold value`.
2. If not, do the usual processing.
3. If it is, upload message content as a blob to external storage, clear message content and add a custom header `x-sn-large` that points to the blob containing message content.

When receiving messages, it will check that `x-sn-large` header is present, and if so, will download blob, set it's content as message content, and return the message to the receiver.

Blob is deleted from the blob storage only when message is confirmed by the receiver.

Large message handling works **on any supported queue implementation** because it's implemented in the core library itself, outside of specific queue implementation. To enable it, call `.HandleLargeContent` on both publisher and receiver:

```csharp
IBlobStorage offloadStorage = ...; // your blob storage for offloading content

IMessagePublisher publisher = StorageFactory.Messages
  .XXXPublisher(...)
  .HandleLargeContent(offloadStorage, thresholdValue);

IMessageReceiver receiver = StorageFactory.Messages
  .XXXReceiver(...)
  .HandleLargeContent(offloadStorage);
```

#### Serialising/deserialising `QueueMessage`

`QueueMessage` class itself is not a serialisable entity when we talk about JSON or built-in .NET binary serialisation due to the fact it is a functionally rich structure. However, you might want to transfer the whole `QueueMessage` across the wire sometimes. For these purposes you can use built-in binary methods:

```csharp
var qm = new QueueMessage("id", "content");
qm.DequeueCount = 4;
qm.Properties.Add("key", "value");

byte[] wireData = qm.ToByteArray();

//transfer the bytes

QueueMessage receivedMessage = QueueMessage.FromByteArray(wireData);
```

These methods make sure that *all* of the message data is preserved, and also are backward compatible between any changes to this class.

## Sponsorship

This framework is free and can be used for free, open source and commercial applications. Storage.Net (all code, NuGets and binaries) are under the [MIT License (MIT)](https://github.com/aloneguid/storage/blob/master/LICENSE). It's battle-tested and used by many awesome people and organisations. So hit the magic ⭐️ button, we appreciate it!!! 🙏 Thx!

The core team members, Storage.Net contributors and contributors in the ecosystem do this open source work in their free time. If you use Storage.Net, and you'd like us to invest more time on it, please donate. This project increases your income/productivity/usabilty too.

### Why charge/sponsor for open source?

 * [Open-Source Maintainers are Jerks! | Nick Randolph & Geoffrey Huntley](https://vimeo.com/296579853)
 * [FOSS is free as in toilet | Geoffroy Couprie](http://unhandledexpression.com/general/2018/11/27/foss-is-free-as-in-toilet.html)
 * [How to Charge for your Open Source | Mike Perham](https://www.mikeperham.com/2015/11/23/how-to-charge-for-your-open-source/)
 * [Sustain OSS: The Report](https://sustainoss.org/assets/pdf/SustainOSS-west-2017-report.pdf)
 * [Open Source Maintainers Owe You Nothing | Mike McQuaid](https://mikemcquaid.com/2018/03/19/open-source-maintainers-owe-you-nothing/)
 * [Who should fund open source projects? | Jane Elizabeth](https://jaxenter.com/who-funds-open-source-projects-133222.html)
 * [Apply at OSS Inc today| Ryan Chenkie](https://twitter.com/ryanchenkie/status/1067801413974032385)
 * [The Ethics of Unpaid Labor and the OSS Community | Ashe Dryden](https://www.ashedryden.com/blog/the-ethics-of-unpaid-labor-and-the-oss-community)

### Backers

Become a backer and show your support to our open source project.

[![](https://opencollective.com/storagedotnet/backers.svg)](https://opencollective.com/storagedotnet#support)

### Sponsors

Does your company use Storage.Net?  Ask your manager or marketing team if your company would be interested in supporting our project.  Support will allow the maintainers to dedicate more time for maintenance and new features for everyone.  Also, your company's logo will show here - who doesn't want a little extra exposure?

[![](https://opencollective.com/storagedotnet/sponsors.svg)](https://opencollective.com/storagedotnet#sponsor)

## Contributing

All contributions of any size and areas are welcome, being it coding, testing, documentation or consulting. The framework is heavily tested under stress with integration tests, in fact most of the code is tests, not implementation, and this approach is more preferred to adding unstable features.

PRs are raised against [develop](https://github.com/aloneguid/storage/tree/develop) branch, **master** is for releases only.

All the builds (from PR or branches) are automatically generating *NuGet packages* which are placed into the following intermediate feed:

`https://pkgs.dev.azure.com/aloneguid/AllPublic/_packaging/storagenet/nuget/v3/index.json`

Builds from *master* branch publishes NuGet to `nuget.org`.

### Code

Storage.Net tries to enforce idential behavior on all implementaions of storage interfaces to the smallest details possible and you will find a lot of test specifications which will help you to add another provider.

The solution is created in Visual Studio 2017 (Community Edition).

### Documentation

When I think of the best way to document a framework I tend to think that working examples are the best. Adding various real world scenarios with working code is more preferrable than just documenting an untested API.

### Reporting bugs or requesting features

Please use the GitHub issue tracker to do this.

### Support

You can get support by raising an issue here, or contacting me directly for consulting services.
