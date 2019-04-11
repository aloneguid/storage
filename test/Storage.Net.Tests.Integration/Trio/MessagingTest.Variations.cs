using System;
using Amazon;
using Storage.Net.Blob;
using Storage.Net.Messaging;
using Xunit;

namespace Storage.Net.Tests.Integration.Messaging
{

   /// <summary>
   /// Azure Storage Queue
   /// </summary>
   public class AzureStorageQueueFixture : MessagingFixture
   {
      protected override IMessagePublisher CreatePublisher(ITestSettings settings) => 
         StorageFactory.Messages.AzureStorageQueuePublisher(
            settings.AzureStorageName,
            settings.AzureStorageKey,
            settings.AzureStorageQueueName);

      protected override IMessageReceiver CreateReceiver(ITestSettings settings) =>
         StorageFactory.Messages.AzureStorageQueueReceiver(
            settings.AzureStorageName,
            settings.AzureStorageKey,
            settings.AzureStorageQueueName,
            TimeSpan.FromMinutes(1),
            TimeSpan.FromMilliseconds(500));
   }

   public class AzureStorageQueueTest : MessagingTest, IClassFixture<AzureStorageQueueFixture>
   {
      public AzureStorageQueueTest(AzureStorageQueueFixture fixture) : base(fixture)
      {
      }
   }

   /// <summary>
   /// Azure Large Storage Queue
   /// </summary>
   public class AzureLargeStorageQueueFixture : MessagingFixture
   {
      protected override IMessagePublisher CreatePublisher(ITestSettings settings)
      {
         string largeQueueName = settings.AzureStorageQueueName + "lg";
         IBlobStorage offloadStorage = StorageFactory.Blobs.AzureBlobStorage(settings.AzureStorageName, settings.AzureStorageKey);

         return StorageFactory.Messages.AzureStorageQueuePublisher(
            settings.AzureStorageName,
            settings.AzureStorageKey,
            largeQueueName)
            .HandleLargeContent(offloadStorage, 2);
      }

      protected override IMessageReceiver CreateReceiver(ITestSettings settings)
      {
         string largeQueueName = settings.AzureStorageQueueName + "lg";
         IBlobStorage offloadStorage = StorageFactory.Blobs.AzureBlobStorage(settings.AzureStorageName, settings.AzureStorageKey);

         return StorageFactory.Messages.AzureStorageQueueReceiver(
            settings.AzureStorageName,
            settings.AzureStorageKey,
            largeQueueName,
            TimeSpan.FromMinutes(1),
            TimeSpan.FromMilliseconds(500))
            .HandleLargeContent(offloadStorage);
      }
   }

   public class AzureLargeStorageQueueTest : MessagingTest, IClassFixture<AzureLargeStorageQueueFixture>
   {
      public AzureLargeStorageQueueTest(AzureLargeStorageQueueFixture fixture) : base(fixture)
      {
      }
   }

   /// <summary>
   /// Azure Service Bus Queue
   /// </summary>
   public class AzureServiceBusQueueFixture : MessagingFixture
   {
      protected override IMessagePublisher CreatePublisher(ITestSettings settings) =>
         StorageFactory.Messages.AzureServiceBusQueuePublisher(
            settings.ServiceBusConnectionString,
            "testqueue");

      protected override IMessageReceiver CreateReceiver(ITestSettings settings) =>
         StorageFactory.Messages.AzureServiceBusQueueReceiver(
            settings.ServiceBusConnectionString,
            "testqueue",
            true);
   }

   public class AzureServiceBusQueueTest : MessagingTest, IClassFixture<AzureServiceBusQueueFixture>
   {
      public AzureServiceBusQueueTest(AzureServiceBusQueueFixture fixture) : base(fixture)
      {
      }
   }

   /// <summary>
   /// Azure Service Bus Topic
   /// </summary>
   public class AzureServiceBusTopicFixture : MessagingFixture
   {
      protected override IMessagePublisher CreatePublisher(ITestSettings settings) =>
         StorageFactory.Messages.AzureServiceBusTopicPublisher(
            settings.ServiceBusConnectionString,
            "testtopic");

      protected override IMessageReceiver CreateReceiver(ITestSettings settings) =>
         StorageFactory.Messages.AzureServiceBusTopicReceiver(
            settings.ServiceBusConnectionString,
            "testtopic",
            "testsub",
            true);
   }

   public class AzureServiceBusTopicTest : MessagingTest, IClassFixture<AzureServiceBusTopicFixture>
   {
      public AzureServiceBusTopicTest(AzureServiceBusTopicFixture fixture) : base(fixture)
      {
      }
   }

   /// <summary>
   /// Azure Event Hub
   /// </summary>
   public class AzureEventHubFixture : MessagingFixture
   {
      protected override IMessagePublisher CreatePublisher(ITestSettings settings) =>
         StorageFactory.Messages.AzureEventHubPublisher(
            settings.EventHubConnectionString,
            settings.EventHubPath);

      protected override IMessageReceiver CreateReceiver(ITestSettings settings) =>
         StorageFactory.Messages.AzureEventHubReceiver(
            settings.EventHubConnectionString,
            settings.EventHubPath,
            null,
            null,
            StorageFactory.Blobs.AzureBlobStorage(
               settings.AzureStorageName,
               settings.AzureStorageKey));
   }

   public class AzureEventHubTest : MessagingTest, IClassFixture<AzureEventHubFixture>
   {
      public AzureEventHubTest(AzureEventHubFixture fixture) : base(fixture)
      {
      }
   }

   /// <summary>
   /// Local Disk Directory
   /// </summary>
   public class LocalDirectoryFixture : MessagingFixture
   {
      protected override IMessagePublisher CreatePublisher(ITestSettings settings) => StorageFactory.Messages.DirectoryFilesPublisher(_testDir);

      protected override IMessageReceiver CreateReceiver(ITestSettings settings) => StorageFactory.Messages.DirectoryFilesReceiver(_testDir);
   }

   public class LocalDirectoryTest : MessagingTest, IClassFixture<LocalDirectoryFixture>
   {
      public LocalDirectoryTest(LocalDirectoryFixture fixture) : base(fixture)
      {
      }
   }

   /// <summary>
   /// Amazon Simple Queue
   /// </summary>
   public class AmazonSimpleQueueFixture : MessagingFixture
   {
      protected override IMessagePublisher CreatePublisher(ITestSettings settings) =>
         StorageFactory.Messages.AmazonSQSMessagePublisher(
            settings.AwsAccessKeyId,
            settings.AwsSecretAccessKey,
            "https://sqs.us-east-1.amazonaws.com",
            "integration",
            RegionEndpoint.USEast1);

      protected override IMessageReceiver CreateReceiver(ITestSettings settings) =>
         StorageFactory.Messages.AmazonSQSMessageReceiver(
            settings.AwsAccessKeyId,
            settings.AwsSecretAccessKey,
               "https://sqs.us-east-1.amazonaws.com",
               "integration",
               RegionEndpoint.USEast1);
   }

   public class AmazonSimpleQueueTest : MessagingTest, IClassFixture<AmazonSimpleQueueFixture>
   {
      public AmazonSimpleQueueTest(AmazonSimpleQueueFixture fixture) : base(fixture)
      {
      }
   }
}
