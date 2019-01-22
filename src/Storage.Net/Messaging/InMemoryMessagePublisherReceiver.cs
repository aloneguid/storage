using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NetBox.Extensions;

namespace Storage.Net.Messaging
{
   class InMemoryMessagePublisherReceiver : PollingMessageReceiver, IMessagePublisher
   {
      private static readonly Dictionary<string, InMemoryMessagePublisherReceiver> _inMemoryMessagingNameToInstance =
         new Dictionary<string, InMemoryMessagePublisherReceiver>();

      private readonly ConcurrentQueue<QueueMessage> _queue = new ConcurrentQueue<QueueMessage>();

      public static InMemoryMessagePublisherReceiver CreateOrGet(string name)
      {
         if (name == null) throw new ArgumentNullException(nameof(name));

         return _inMemoryMessagingNameToInstance.GetOrAdd(name, () => new InMemoryMessagePublisherReceiver());
      }

      public Task PutMessagesAsync(IEnumerable<QueueMessage> messages, CancellationToken cancellationToken = default)
      {
         if (messages == null) return Task.FromResult(true);

         foreach(QueueMessage qm in messages)
         {
            _queue.Enqueue(qm);
         }

         return Task.FromResult(true);
      }

      protected override Task<IReadOnlyCollection<QueueMessage>> ReceiveMessagesAsync(int maxBatchSize, CancellationToken cancellationToken)
      {
         var result = new List<QueueMessage>();

         while(_queue.TryDequeue(out QueueMessage message) && result.Count < maxBatchSize)
         {
            result.Add(message);
         }

         return Task.FromResult((IReadOnlyCollection<QueueMessage>)result);
      }

      public override Task ConfirmMessagesAsync(IReadOnlyCollection<QueueMessage> messages, CancellationToken cancellationToken = default)
      {
         return Task.FromResult(true);
      }

      public override Task<int> GetMessageCountAsync() => Task.FromResult(_queue.Count);
   }
}