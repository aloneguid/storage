using System;
using Storage.Net.Blob;
using Storage.Net.Messaging;
using Storage.Net.KeyValue;
using System.Net;
using Storage.Net.Microsoft.Azure.EventHub;
using System.Collections.Generic;
using EHP = Storage.Net.Microsoft.Azure.EventHub.AzureEventHubPublisher;
using EHR = Storage.Net.Microsoft.Azure.EventHub.AzureEventHubReceiver;

namespace Storage.Net
{
   /// <summary>
   /// Factory class that implement factory methods for Microsoft Azure implememtations
   /// </summary>
   public static class Factory
   {
      /// <summary>
      /// Creates Azure Event Hub publisher by namespace connection string and hub path
      /// </summary>
      /// <param name="factory">Factory reference</param>
      /// <param name="connectionString">Connection string</param>
      /// <param name="hubPath">Hub path (name)</param>
      /// <returns>Message publisher</returns>
      public static IMessagePublisher AzureEventHubPublisher(this IMessagingFactory factory, string connectionString, string hubPath)
      {
         return EHP.Create(connectionString, hubPath);
      }

      /// <summary>
      /// Create Azure Event Hub publisher by full connection string
      /// </summary>
      /// <param name="factory">Factory reference</param>
      /// <param name="fullConnectionString">Connection string</param>
      public static IMessagePublisher AzureEventHubPublisher(this IMessagingFactory factory, string fullConnectionString)
      {
         return new EHP(fullConnectionString);
      }

      /// <summary>
      /// The most detailed method with full fragmentation
      /// </summary>
      /// <param name="factory">Factory reference</param>
      /// <param name="endpointAddress">Endpoint address</param>
      /// <param name="entityPath">Entity path</param>
      /// <param name="sharedAccessKeyName">Shared access key name</param>
      /// <param name="sharedAccessKey">Shared access key value</param>
      /// <returns></returns>
      public static IMessagePublisher AzureEventHubPublisher(this IMessagingFactory factory, Uri endpointAddress, string entityPath, string sharedAccessKeyName, string sharedAccessKey)
      {
         return EHP.Create(endpointAddress, entityPath, sharedAccessKeyName, sharedAccessKey);
      }

      /// <summary>
      /// Creates Azure Event Hub receiver
      /// </summary>
      /// <param name="factory">Factory reference</param>
      /// <param name="connectionString"></param>
      /// <param name="hubPath"></param>
      /// <param name="partitionIds"></param>
      /// <param name="consumerGroupName"></param>
      /// <param name="stateStorage"></param>
      /// <returns></returns>
      public static IMessageReceiver AzureEventHubReceiver(this IMessagingFactory factory,
         string connectionString, string hubPath,
         IEnumerable<string> partitionIds = null,
         string consumerGroupName = null,
         IBlobStorage stateStorage = null
         )
      {
         return new AzureEventHubReceiver(connectionString, hubPath, partitionIds, consumerGroupName, stateStorage);
      }
   }
}
