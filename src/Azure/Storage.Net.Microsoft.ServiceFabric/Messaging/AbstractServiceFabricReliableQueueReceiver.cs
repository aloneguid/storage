using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using Storage.Net.Messaging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Storage.Net.Microsoft.ServiceFabric.Messaging
{
   abstract class AbstractServiceFabricReliableQueueReceiver : IMessageReceiver
   {
      private readonly IReliableStateManager _stateManager;
      private readonly string _queueName;
      private readonly TimeSpan _scanInterval;
      private bool _disposed;

      protected AbstractServiceFabricReliableQueueReceiver(IReliableStateManager stateManager, string queueName, TimeSpan scanInterval)
      {
         _stateManager = stateManager ?? throw new ArgumentNullException(nameof(stateManager));
         _queueName = queueName ?? throw new ArgumentNullException(nameof(queueName));

         if (scanInterval < TimeSpan.FromSeconds(1)) throw new ArgumentException("scan interval must be at least 1 second", nameof(scanInterval));

         _scanInterval = scanInterval;
      }

      /// <summary>
      /// See interface
      /// </summary>
      public async Task<int> GetMessageCountAsync()
      {
         using (var tx = new ServiceFabricTransaction(_stateManager, null))
         {
            IReliableState collection = await GetCollectionAsync();

            return await GetMessageCountAsync(collection, tx);
         }
      }

      protected abstract Task<int> GetMessageCountAsync(IReliableState reliableState, ServiceFabricTransaction transaction);

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
      public async Task StartMessagePumpAsync(Func<IReadOnlyCollection<QueueMessage>, Task> onMessage, int maxBatchSize, CancellationToken cancellationToken)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
      {
#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
         Task.Run(() => ReceiveMessagesAsync(onMessage, maxBatchSize, cancellationToken));
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
      }

      public Task ConfirmMessagesAsync(IReadOnlyCollection<QueueMessage> message, CancellationToken cancellationToken)
      {
         return Task.FromResult(true);
      }

      public Task DeadLetterAsync(QueueMessage message, string reason, string errorDescription, CancellationToken cancellationToken)
      {
         return Task.FromResult(true);
      }

      protected abstract Task<ConditionalValue<byte[]>> TryDequeueAsync(ServiceFabricTransaction tx, IReliableState collectionBase, CancellationToken cancellationToken);

      private async Task ReceiveMessagesAsync(Func<IReadOnlyCollection<QueueMessage>, Task> onMessage, int maxBatchSize, CancellationToken cancellationToken)
      {

         while (!cancellationToken.IsCancellationRequested && !_disposed)
         {
            try
            {
               using (var tx = new ServiceFabricTransaction(_stateManager, null))
               {
                  IReliableState collection = await GetCollectionAsync();

                  var messages = new List<QueueMessage>();

                  while (messages.Count < maxBatchSize)
                  {
                     ConditionalValue<byte[]> message = await TryDequeueAsync(tx, collection, cancellationToken);
                     if (message.HasValue)
                     {
                        QueueMessage qm = QueueMessage.FromByteArray(message.Value);

                        messages.Add(qm);
                     }
                     else
                     {
                        break;
                     }
                  }

                  //make the call before committing the transaction
                  if (messages.Count > 0)
                  {
                     await onMessage(messages);
                  }

                  await tx.CommitAsync();
               }
            }
            catch(Exception ex)
            {
               Trace.Fail($"failed to listen to messages on queue '{_queueName}'", ex.ToString());
            }

            await Task.Delay(_scanInterval);
         }

         Trace.TraceInformation("queue '{0}' scanner exited", _queueName);
      }

      public void Dispose()
      {
         _disposed = true;
      }

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
      public async Task<ITransaction> OpenTransactionAsync()
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
      {
         return EmptyTransaction.Instance;
      }

      protected abstract Task<IReliableState> GetCollectionAsync();
   }
}
