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
   class ServiceFabricReliableQueueReceiver : AbstractServiceFabricReliableQueueReceiver
   {
      private readonly IReliableStateManager _stateManager;
      private readonly string _queueName;

      public ServiceFabricReliableQueueReceiver(IReliableStateManager stateManager, string queueName, TimeSpan scanInterval)
         : base(stateManager, queueName, scanInterval)
      {
         _stateManager = stateManager;
         _queueName = queueName;
      }

      protected override async Task<IReliableState> GetCollectionAsync()
      {
         return await _stateManager.GetOrAddAsync<IReliableQueue<byte[]>>(_queueName);
      }

      protected override async Task<int> GetMessageCountAsync(IReliableState reliableState, ServiceFabricTransaction transaction)
      {
         IReliableQueue<byte[]> collection = (IReliableQueue<byte[]>)reliableState;

         long count = await collection.GetCountAsync(transaction.Tx);

         return (int)count;
      }

      protected override async Task<ConditionalValue<byte[]>> TryDequeueAsync(ServiceFabricTransaction tx, IReliableState collectionBase, CancellationToken cancellationToken)
      {
         IReliableQueue<byte[]> collection = (IReliableQueue<byte[]>)collectionBase;

         ConditionalValue<byte[]> message = await collection.TryDequeueAsync(tx.Tx, TimeSpan.FromSeconds(4), cancellationToken);

         return message;
      }
   }
}
