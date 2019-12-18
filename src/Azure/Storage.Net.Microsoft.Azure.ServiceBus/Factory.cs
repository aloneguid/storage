using System;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;
using Storage.Net.Messaging;
using Storage.Net.Microsoft.Azure.ServiceBus;
using Storage.Net.Microsoft.Azure.ServiceBus.Messaging;

namespace Storage.Net
{
   /// <summary>
   /// Factory class that implement factory methods for Microsoft Azure implememtations
   /// </summary>
   public static class Factory
   {

      /// <summary>
      /// Creates a new instance of Azure Service Bus Queue by connection string and queue name
      /// </summary>
      /// <param name="factory">Factory reference</param>
      /// <param name="connectionString">Service Bus connection string pointing to a namespace or an entity</param>
      public static IMessenger AzureServiceBus(this IMessagingFactory factory,
         string connectionString)
      {
         return new AzureServiceBusMessenger(connectionString);
      }

      /// <summary>
      /// Creates Azure Service Bus Receiver
      /// </summary>
      public static IMessageReceiver AzureServiceBusTopicReceiver(this IMessagingFactory factory,
         string connectionString,
         string topicName,
         string subscriptionName,
         bool peekLock = true,
         MessageHandlerOptions messageHandlerOptions = null)
      {
         return new AzureServiceBusTopicReceiver(connectionString, topicName, subscriptionName, peekLock, messageHandlerOptions);
      }
   }
}
