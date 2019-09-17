﻿using System;
using Storage.Net.Blobs;
using Storage.Net.Messaging;
using Storage.Net.KeyValue;
using System.Net;
using Storage.Net.Microsoft.Azure.EventHub;
using System.Collections.Generic;
namespace Storage.Net
{
   /// <summary>
   /// Factory class that implement factory methods for Microsoft Azure implememtations
   /// </summary>
   public static class Factory
   {
      /// <summary>
      /// Create Azure Event Hub publisher by full connection string
      /// </summary>
      /// <param name="factory">Factory reference</param>
      /// <param name="fullConnectionString">Connection string</param>
      public static IMessenger AzureEventHubPublisher(this IMessagingFactory factory, string fullConnectionString)
      {
         return new AzureEventHubMessenger(fullConnectionString);
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
