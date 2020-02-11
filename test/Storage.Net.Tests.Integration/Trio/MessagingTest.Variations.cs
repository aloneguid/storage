using System;
using Amazon;
using Storage.Net.Blobs;
using Storage.Net.Messaging;
using Xunit;

namespace Storage.Net.Tests.Integration.Messaging
{

   #region [ Azure Storage Queue ]

   public class AzureStorageQueueFixture : MessagingFixture
   {
      protected override IMessenger CreateMessenger(ITestSettings settings) =>
         StorageFactory.Messages.AzureStorageQueue(
            settings.AzureStorageName,
            settings.AzureStorageKey);
   }

   public class AzureStorageQueueTest : MessagingTest, IClassFixture<AzureStorageQueueFixture>
   {
      public AzureStorageQueueTest(AzureStorageQueueFixture fixture) : base(fixture)
      {
      }
   }

   #endregion

   #region [ In-Memory ]

   public class InMemoryFixture : MessagingFixture
   {
      protected override IMessenger CreateMessenger(ITestSettings settings)
      {
         return StorageFactory.Messages.InMemory("test");
      }
   }

   public class InMemoryTest : MessagingTest, IClassFixture<InMemoryFixture>
   {
      public InMemoryTest(InMemoryFixture fixture) : base(fixture)
      {
      }
   }

   #endregion

   #region [ Disk ]

   public class DiskFixture : MessagingFixture
   {
      protected override IMessenger CreateMessenger(ITestSettings settings)
      {
         return StorageFactory.Messages.Disk(_testDir);
      }
   }

   public class DiskTest : MessagingTest, IClassFixture<DiskFixture>
   {
      public DiskTest(DiskFixture fixture) : base(fixture)
      {
      }
   }

   #endregion

   #region [ AWS SQS ]

   public class AwsSQSFixture : MessagingFixture
   {
      protected override IMessenger CreateMessenger(ITestSettings settings)
      {
         return StorageFactory.Messages.AwsSQS(
            settings.AwsAccessKeyId,
            settings.AwsSecretAccessKey,
            "https://sqs.us-east-1.amazonaws.com",
            RegionEndpoint.USEast1);
      }
   }


   public class AwsSQSTest : MessagingTest, IClassFixture<AwsSQSFixture>
   {
      public AwsSQSTest(AwsSQSFixture fixture) : base(fixture)
      {
      }
   }
   #endregion

   #region [ Azure Service Bus ]

   public class AzureServiceBusFixture : MessagingFixture
   {
      protected override IMessenger CreateMessenger(ITestSettings settings)
      {
         return StorageFactory.Messages.AzureServiceBus(settings.AzureServiceBusConnectionString);

      }
   }

   public class AzureServiceBusTopicTest : MessagingTest, IClassFixture<AzureServiceBusFixture>
   {
      public AzureServiceBusTopicTest(AzureServiceBusFixture fixture) : base(fixture, "t/", "t/fxtopic", receiveChannelSuffix: "/default")
      {
      }
   }

   public class AzureServiceBusQueueTest : MessagingTest, IClassFixture<AzureServiceBusFixture>
   {
      public AzureServiceBusQueueTest(AzureServiceBusFixture fixture) : base(fixture, "q/", "q/fxqueue")
      {
      }
   }

   #endregion

   #region [ Azure Event Hubs ]

   public class AzureEventHubFixture : MessagingFixture
   {
      protected override IMessenger CreateMessenger(ITestSettings settings)
      {
         return StorageFactory.Messages.AzureEventHub(settings.AzureEventHubConnectionString + ";EntityPath=integration");
      }
   }

   public class AzureEventHubTest : MessagingTest, IClassFixture<AzureEventHubFixture>
   {
      public AzureEventHubTest(AzureEventHubFixture fixture) : base(fixture, channelFixedName: "integration")
      {
      }
   }

   #endregion

}