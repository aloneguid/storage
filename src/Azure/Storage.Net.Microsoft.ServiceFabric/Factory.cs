using Microsoft.ServiceFabric.Data;
using Storage.Net.Blobs;
using Storage.Net.Messaging;
using Storage.Net.Microsoft.ServiceFabric.Blobs;
using Storage.Net.Microsoft.ServiceFabric.Messaging;
using System;

namespace Storage.Net
{
   public static class Factory
   {
      private const string DefaultQueueName = "$default$";

      public static IBlobStorage AzureServiceFabricReliableStorage(this IBlobStorageFactory factory,
         IReliableStateManager stateManager,
         string collectionName)
      {
         return new ServiceFabricReliableDictionaryBlobStorageProvider(stateManager, collectionName);
      }

      public static IMessenger AzureServiceFabricReliableQueueMessenger(
         this IMessagingFactory factory,
         IReliableStateManager stateManager,
         string queueName = null)
      {
         return new ServiceFabricReliableQueuePublisher(stateManager, queueName ?? DefaultQueueName);
      }

      public static IMessenger AzureServiceFabricReliableConcurrentQueueMessenger(
         this IMessagingFactory factory,
         IReliableStateManager stateManager,
         string queueName = null)
      {
         return new ServiceFabricReliableConcurrentQueuePublisher(stateManager, queueName ?? DefaultQueueName);
      }
   }
}
