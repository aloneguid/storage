using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs.Processor;
using Storage.Net.Messaging;

namespace Storage.Net.Microsoft.Azure.EventHub
{
   /// <summary>
   /// Understand event processor host - https://docs.microsoft.com/en-gb/azure/event-hubs/event-hubs-event-processor-host
   /// </summary>
   class AzureEventHubProcessorHostReceiver : IMessageReceiver
   {
      private readonly EventProcessorHost _host;

      public AzureEventHubProcessorHostReceiver(string connectionString, string hubPath,
         string storageConnectionString,
         string leaseContainerName,
         string consumerGroupName = null)
      {
         _host = new EventProcessorHost(hubPath, consumerGroupName, connectionString, storageConnectionString, leaseContainerName);
      }

      public Task ConfirmMessagesAsync(IReadOnlyCollection<QueueMessage> messages, CancellationToken cancellationToken = default)
      {
         return Task.CompletedTask;
      }

      public Task DeadLetterAsync(QueueMessage message, string reason, string errorDescription, CancellationToken cancellationToken = default)
      {
         throw new NotSupportedException();
      }

      public void Dispose()
      {
         //nothing to dispose
      }

      public Task<int> GetMessageCountAsync()
      {
         throw new NotSupportedException();
      }

      public Task<ITransaction> OpenTransactionAsync() => Task.FromResult(EmptyTransaction.Instance);

      public Task StartMessagePumpAsync(
         Func<IReadOnlyCollection<QueueMessage>, Task> onMessageAsync,
         int maxBatchSize = 1,
         CancellationToken cancellationToken = default)
      {
         //_host.RegisterEventProcessorAsync()
         throw new NotImplementedException();
      }
   }
}
