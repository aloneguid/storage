using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.EventHubs.Processor;
using Storage.Net.Messaging;

namespace Storage.Net.Microsoft.Azure.EventHub
{
   /// <summary>
   /// Receive events using event processor host (in progress)
   /// </summary>
   class AzureEventHubProcessor : IEventProcessor
   {
      private readonly Func<IReadOnlyCollection<QueueMessage>, Task> _onMessageAsync;

      public AzureEventHubProcessor(Func<IReadOnlyCollection<QueueMessage>, Task> onMessageAsync)
      {
         _onMessageAsync = onMessageAsync ?? throw new ArgumentNullException(nameof(onMessageAsync));
      }

      public Task CloseAsync(PartitionContext context, CloseReason reason)
      {
         return Task.CompletedTask;
      }

      public Task OpenAsync(PartitionContext context)
      {
         return Task.CompletedTask;
      }

      public Task ProcessErrorAsync(PartitionContext context, Exception error)
      {
         return Task.CompletedTask;
      }

      public async Task ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> messages)
      {
         List<QueueMessage> qms = messages.Select(ed => Converter.ToQueueMessage(ed, context.PartitionId)).ToList();
         await _onMessageAsync(qms);

         await context.CheckpointAsync();
      }
   }
}
