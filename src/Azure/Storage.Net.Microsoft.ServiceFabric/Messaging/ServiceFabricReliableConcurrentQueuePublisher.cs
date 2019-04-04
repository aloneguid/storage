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
   class ServiceFabricReliableConcurrentQueuePublisher : IMessagePublisher
   {
      private readonly IReliableStateManager _stateManager;
      private readonly string _queueName;
      private readonly TimeSpan? _timeout;

      public ServiceFabricReliableConcurrentQueuePublisher(IReliableStateManager stateManager, string queueName, TimeSpan? timeout = null)
      {
         _stateManager = stateManager ?? throw new ArgumentNullException(nameof(stateManager));
         _queueName = queueName ?? throw new ArgumentNullException(nameof(queueName));
         _timeout = timeout;
      }

      public async Task PutMessagesAsync(IReadOnlyCollection<QueueMessage> messages, CancellationToken cancellationToken)
      {
         IReliableConcurrentQueue<byte[]> collection = await _stateManager.GetOrAddAsync<IReliableConcurrentQueue<byte[]>>(_queueName);

         using (var tx = new ServiceFabricTransaction(_stateManager, null))
         {
            foreach (QueueMessage message in messages)
            {
               byte[] data = message.ToByteArray();
               await collection.EnqueueAsync(tx.Tx, data, cancellationToken, _timeout);
            }

            await tx.CommitAsync();
         }
      }

      public void Dispose()
      {

      }
   }
}
