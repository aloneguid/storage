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
   class MessageProcessorEventProcessor : IEventProcessor
   {
      private readonly IMessageProcessor _messageProcessor;
      private DateTimeOffset _lastCheckpointTime;
      private static readonly TimeSpan CheckpointInterval = TimeSpan.FromMinutes(1);

      public MessageProcessorEventProcessor(IMessageProcessor messageProcessor, PartitionContext partitionContext)
      {
         _messageProcessor = messageProcessor ?? throw new ArgumentNullException(nameof(messageProcessor));
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
         //forward to message processor
         List<QueueMessage> qms = messages.Select(ed => Converter.ToQueueMessage(ed, context.PartitionId)).ToList();

         await _messageProcessor.ProcessMessagesAsync(qms).ConfigureAwait(false);

         //checkpoint
         DateTimeOffset now = DateTimeOffset.Now;
         if(now - _lastCheckpointTime >= CheckpointInterval)
         {
            await context.CheckpointAsync().ConfigureAwait(false);

            _lastCheckpointTime = now;
         }

      }
   }
}
