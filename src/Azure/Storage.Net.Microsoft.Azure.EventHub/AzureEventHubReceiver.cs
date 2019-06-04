using Storage.Net.Messaging;
using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using Microsoft.Azure.EventHubs;
using Storage.Net.Blob;
using System.Threading;
using System.Linq;
using NetBox.Extensions;

namespace Storage.Net.Microsoft.Azure.EventHub
{
   /// <summary>
   /// Microsoft Azure Event Hub receiver
   /// </summary>
   class AzureEventHubReceiver : IMessageReceiver
   {
      private readonly EventHubClient _hubClient;
      private readonly HashSet<string> _partitionIds;
      private readonly string _consumerGroupName;
      private readonly EventHubStateAdapter _state;
      private static readonly TimeSpan _waitTime = TimeSpan.FromSeconds(1);

      /// <summary>
      /// Creates an instance of EventHub receiver
      /// </summary>
      /// <param name="connectionString">Event hub connection string</param>
      /// <param name="hubPath">Entity path</param>
      /// <param name="partitionIds">When specified, listens only on specified partition(s), otherwise all partitions will be used</param>
      /// <param name="consumerGroupName">When not specified uses default consumer group</param>
      /// <param name="stateStorage">When specified uses this storage for persisting the state i.e. will be able to continue from
      /// where it left next time it runs</param>
      public AzureEventHubReceiver(string connectionString, string hubPath,
         IEnumerable<string> partitionIds = null,
         string consumerGroupName = null,
         IBlobStorage stateStorage = null)
      {
         if (connectionString == null)
            throw new ArgumentNullException(nameof(connectionString));

         if (hubPath == null)
            throw new ArgumentNullException(nameof(hubPath));

         _partitionIds = partitionIds == null ? null : new HashSet<string>(partitionIds);

         var builder = new EventHubsConnectionStringBuilder(connectionString) { EntityPath = hubPath };
         _hubClient = EventHubClient.CreateFromConnectionString(builder.ToString());
         if (partitionIds != null) _partitionIds.AddRange(_partitionIds);
         _consumerGroupName = consumerGroupName;
         _state = new EventHubStateAdapter(stateStorage ?? StorageFactory.Blobs.InMemory());
      }

      /// <summary>
      /// Gets the IDs of event hub partitions
      /// </summary>
      /// <returns></returns>
      public async Task<IEnumerable<string>> GetPartitionIdsAsync()
      {
         EventHubRuntimeInformation info = await _hubClient.GetRuntimeInformationAsync();

         return info.PartitionIds;
      }

      private async Task<IEnumerable<PartitionReceiver>> CreateReceiversAsync()
      {
         var receivers = new List<PartitionReceiver>();

         EventHubRuntimeInformation runtimeInfo = await _hubClient.GetRuntimeInformationAsync();

         foreach (string partitionId in runtimeInfo.PartitionIds)
         {
            if (_partitionIds == null || _partitionIds.Contains(partitionId))
            {
               PartitionReceiver receiver = _hubClient.CreateReceiver(
                  _consumerGroupName ?? PartitionReceiver.DefaultConsumerGroupName,
                  partitionId,
                  (await _state.GetPartitionPosition(partitionId)));

               receivers.Add(receiver);
            }
         }

         return receivers;
      }

      /// <summary>
      /// See interface
      /// </summary>
      public Task<int> GetMessageCountAsync()
      {
         throw new NotSupportedException();
      }

      /// <summary>
      /// See interface
      /// </summary>
      public Task ConfirmMessagesAsync(IReadOnlyCollection<QueueMessage> messages, CancellationToken cancellationToken = default)
      {
         //nothing to confirm
         return Task.FromResult(true);
      }

      /// <summary>
      /// See interface
      /// </summary>
      public Task DeadLetterAsync(QueueMessage message, string reason, string errorDescription, CancellationToken cancellationToken = default)
      {
         //no dead letter queue in EH
         return Task.FromResult(true);
      }

      /// <summary>
      /// See interface
      /// </summary>
      public async Task StartMessagePumpAsync(Func<IReadOnlyCollection<QueueMessage>, Task> onMessageAsync, int maxBatchSize = 1, CancellationToken cancellationToken = default)
      {
         IEnumerable<PartitionReceiver> receivers = await CreateReceiversAsync();

         foreach(PartitionReceiver receiver in receivers)
         {
            Task pump = ReceiverPumpAsync(receiver, onMessageAsync, maxBatchSize, cancellationToken);
         }
      }

      private async Task ReceiverPumpAsync(PartitionReceiver receiver, Func<IReadOnlyCollection<QueueMessage>, Task> onMessage, int maxBatchSize, CancellationToken cancellationToken)
      {
         while (true)
         {
            try
            {

               IEnumerable<EventData> events = await receiver.ReceiveAsync(maxBatchSize, _waitTime);

               if (events != null && !cancellationToken.IsCancellationRequested)
               {
                  List<QueueMessage> qms = events.Select(ed => Converter.ToQueueMessage(ed, receiver.PartitionId)).ToList();
                  await onMessage(qms);

                  QueueMessage lastMessage = qms.LastOrDefault();

                  //save state
                  if (lastMessage != null)
                  {
                     const string sequenceNumberPropertyName = "x-opt-sequence-number";

                     if (lastMessage.Properties.TryGetValue(sequenceNumberPropertyName, out string sequenceNumber))
                     {
                        long? sequenceNumberLong = null;
                        if(long.TryParse(sequenceNumber, out long seqenceNumberNonNullable))
                        {
                           sequenceNumberLong = seqenceNumberNonNullable;
                        }

                        await _state.SetPartitionStateAsync(receiver.PartitionId, sequenceNumberLong);
                     }
                  }
               }

               if (cancellationToken.IsCancellationRequested)
               {
                  await receiver.CloseAsync();
                  return;
               }

            }
            catch(ArgumentException ex)
            {
               Console.WriteLine("failed with message: '{0}', clearing partition state.", ex);

               await _state.SetPartitionStateAsync(receiver.PartitionId, EventPosition.FromStart().SequenceNumber);
            }
            catch(OperationCanceledException)
            {
               return;
            }
            catch (Exception ex)
            {
               Console.WriteLine("receiver stopped: {0}", ex);

               return;
            }
         }
      }

      /// <summary>
      /// Cancels message pump and closes the client
      /// </summary>
      public void Dispose()
      {
         //nothing to dispose
      }

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
      public async Task<ITransaction> OpenTransactionAsync()
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
      {
         return EmptyTransaction.Instance;
      }

      /// <summary>
      /// Event Hubs don't have a concept of keeping alive, ignored.
      /// </summary>
      public Task KeepAliveAsync(QueueMessage message, TimeSpan? timeToLive = null, CancellationToken cancellationToken = default) => Task.CompletedTask;
   }
}
