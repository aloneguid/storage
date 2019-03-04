using System;
using System.Collections.Generic;
using System.Linq;
using Storage.Net.Messaging;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;
using System.Threading;

namespace Storage.Net.Microsoft.Azure.ServiceBus.Messaging
{
   /// <summary>
   /// Implements Azure Service Bus Queue
   /// </summary>
   public class AzureServiceBusTopicPublisher : IMessagePublisher
   {
      private readonly string _entityPath;
      private readonly TopicClient _client;

      /// <summary>
      /// Creates a new instance of Azure Service Bus Queue by connection string and queue name
      /// </summary>
      /// <param name="connectionString">Service Bus connection string</param>
      /// <param name="entityPath">Queue name in Service Bus. If queue doesn't exist it will be created for you.</param>
      public AzureServiceBusTopicPublisher(string connectionString, string entityPath)
      {
         _client = new TopicClient(connectionString, entityPath);
         _entityPath = entityPath;
      }

      /// <summary>
      /// Puts message to the queue with default options
      /// </summary>
      public async Task PutMessagesAsync(IEnumerable<QueueMessage> messages, CancellationToken cancellationToken)
      {
         if (messages == null) return;

         IList<Message> sbmsg = messages.Select(Converter.ToMessage).ToList();
         await _client.SendAsync(sbmsg);
      }

      /// <summary>
      /// Closes connection to the queue
      /// </summary>
      public void Dispose()
      {
         _client.CloseAsync().Wait();
      }
   }
}