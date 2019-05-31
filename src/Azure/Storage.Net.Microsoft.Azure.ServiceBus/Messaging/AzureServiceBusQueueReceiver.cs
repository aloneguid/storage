using Microsoft.Azure.ServiceBus;
using Storage.Net.Messaging;
using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Microsoft.Azure.ServiceBus.Core;

namespace Storage.Net.Microsoft.Azure.ServiceBus.Messaging
{
   /// <summary>
   /// Implements message receiver on Azure Service Bus Queues
   /// </summary>
   class AzureServiceBusQueueReceiver : AzureServiceBusReceiver
   {
      //https://github.com/Azure/azure-service-bus/blob/master/samples/DotNet/Microsoft.Azure.ServiceBus/ReceiveSample/readme.md
      public AzureServiceBusQueueReceiver(string connectionString, string queueName, bool peekLock = true, MessageHandlerOptions handlerOptions = null)
         : base(
              CreateClient(connectionString, queueName, peekLock),
              CreateMessageReceiver(connectionString, queueName, peekLock),
              handlerOptions)
      {
      }

      private static QueueClient CreateClient(string connectionString, string queueName, bool peekLock)
      {
         ReceiveMode mode = peekLock ? ReceiveMode.PeekLock : ReceiveMode.ReceiveAndDelete;

         return new QueueClient(connectionString, queueName, mode);
      }
   }
}
