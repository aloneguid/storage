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
      /// <param name="connectionString">Service Bus connection string</param>
      /// <param name="queueName">Queue name in Service Bus. If queue doesn't exist it will be created for you.</param>
      public static IMessagePublisher AzureServiceBusQueuePublisher(this IMessagingFactory factory,
         string connectionString,
         string queueName)
      {
         return new AzureServiceBusQueuePublisher(connectionString, queueName, true);
      }

      /// <summary>
      /// Creates an instance of Azure Service Bus receiver with connection
      /// </summary>
      /// <param name="factory">Factory reference</param>
      /// <param name="connectionString">Service Bus connection string</param>
      /// <param name="queueName">Queue name in Service Bus</param>
      /// <param name="peekLock">When true listens in PeekLock mode, otherwise ReceiveAndDelete</param>
      /// <param name="messageHandlerOptions">Optional native message handler options if you need to override them</param>
      public static IMessageReceiver AzureServiceBusQueueReceiver(this IMessagingFactory factory,
         string connectionString, string queueName, bool peekLock = true,
         MessageHandlerOptions messageHandlerOptions = null)
      {
         return new AzureServiceBusQueueReceiver(connectionString, queueName, peekLock, messageHandlerOptions);
      }

      /// <summary>
      /// Creates an instance of Azure Service Bus Topic publisher.
      /// </summary>
      public static IMessagePublisher AzureServiceBusTopicPublisher(this IMessagingFactory factory,
         string connectionString,
         string topicName)
      {
         return new AzureServiceBusTopicPublisher(connectionString, topicName);
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
