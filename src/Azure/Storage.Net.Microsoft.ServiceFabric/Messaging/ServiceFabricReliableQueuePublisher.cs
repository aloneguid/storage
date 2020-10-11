using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using Storage.Net.Messaging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Storage.Net.Microsoft.ServiceFabric.Messaging
{
   class ServiceFabricReliableQueuePublisher : IMessenger
   {
      private readonly IReliableStateManager _stateManager;
      private readonly string _queueName;
      private readonly TimeSpan _timeout;

      public ServiceFabricReliableQueuePublisher(IReliableStateManager stateManager, string queueName, TimeSpan? timeout = null)
      {
         _stateManager = stateManager ?? throw new ArgumentNullException(nameof(stateManager));
         _queueName = queueName ?? throw new ArgumentNullException(nameof(queueName));
         _timeout = timeout == null ? TimeSpan.FromSeconds(4) : timeout.Value;
      }

      public async Task PutMessagesAsync(IReadOnlyCollection<QueueMessage> messages, CancellationToken cancellationToken)
      {
         IReliableQueue<byte[]> collection = await _stateManager.GetOrAddAsync<IReliableQueue<byte[]>>(_queueName).ConfigureAwait(false);

         using (var tx = new ServiceFabricTransaction(_stateManager, null))
         {
            foreach (QueueMessage message in messages)
            {
               byte[] data = message.ToByteArray();
               await collection.EnqueueAsync(tx.Tx, data, _timeout, cancellationToken).ConfigureAwait(false);
            }

            await tx.CommitAsync().ConfigureAwait(false);
         }
      }

      public void Dispose()
      {

      }

      public Task CreateChannelsAsync(IEnumerable<string> channelNames, CancellationToken cancellationToken = default) => throw new NotImplementedException();
      public Task<IReadOnlyCollection<string>> ListChannelsAsync(CancellationToken cancellationToken = default) => throw new NotImplementedException();
      public Task DeleteChannelsAsync(IEnumerable<string> channelNames, CancellationToken cancellationToken = default) => throw new NotImplementedException();
      public Task<long> GetMessageCountAsync(string channelName, CancellationToken cancellationToken = default) => throw new NotImplementedException();
      public Task SendAsync(string channelName, IEnumerable<QueueMessage> messages, CancellationToken cancellationToken = default) => throw new NotImplementedException();
      public Task<IReadOnlyCollection<QueueMessage>> ReceiveAsync(string channelName, int count = 100, TimeSpan? visibility = null, CancellationToken cancellationToken = default) => throw new NotImplementedException();
      public Task<IReadOnlyCollection<QueueMessage>> PeekAsync(string channelName, int count = 100, CancellationToken cancellationToken = default) => throw new NotImplementedException();
      public Task DeleteAsync(string channelName, IEnumerable<QueueMessage> messages, CancellationToken cancellationToken = default) => throw new NotImplementedException();
      public Task StartMessageProcessorAsync(string channelName, IMessageProcessor messageProcessor) => throw new NotImplementedException();
   }
}
