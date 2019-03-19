using Microsoft.Azure.ServiceBus;
using Storage.Net.Messaging;
using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Storage.Net.Microsoft.Azure.ServiceBus.Messaging
{
   /// <summary>
   /// Implements message receiver on Azure Service Bus Queues
   /// </summary>
   class AzureServiceBusTopicReceiver : IMessageReceiver
   {
      //https://github.com/Azure/azure-service-bus/blob/master/samples/DotNet/Microsoft.Azure.ServiceBus/ReceiveSample/readme.md

      private static readonly TimeSpan AutoRenewDuration = TimeSpan.FromMinutes(1);

      private readonly SubscriptionClient _client;
      private readonly bool _peekLock;
      private readonly AzureReceiverOptions _azureRegisterMessageOptions;
      private readonly ConcurrentDictionary<string, Message> _messageIdToBrokeredMessage = new ConcurrentDictionary<string, Message>();

      /// <summary>
      /// Creates an instance of Azure Service Bus receiver with connection
      /// </summary>
      public AzureServiceBusTopicReceiver(string connectionString, string topicName, string subscriptionName, bool peekLock = true)
         : this(connectionString, topicName, subscriptionName, new AzureReceiverOptions(), peekLock)
      {
      }

      public AzureServiceBusTopicReceiver(string connectionString, string topicName, string subscriptionName, AzureReceiverOptions azureRegisterMessageOptions, bool peekLock = true)
      {
         _client = new SubscriptionClient(connectionString, topicName, subscriptionName, peekLock ? ReceiveMode.PeekLock : ReceiveMode.ReceiveAndDelete);
         _azureRegisterMessageOptions = azureRegisterMessageOptions;
         _peekLock = peekLock;
         _messageHandlerOptions = messageHandlerOptions;
      }

      public Task<int> GetMessageCountAsync()
      {
         throw new NotSupportedException();
      }

      /// <summary>
      /// Calls .DeadLetter explicitly
      /// </summary>
      public async Task DeadLetterAsync(QueueMessage message, string reason, string errorDescription, CancellationToken cancellationToken)
      {
         if (!_peekLock) return;

         if (!_messageIdToBrokeredMessage.TryRemove(message.Id, out Message bm)) return;

         await _client.DeadLetterAsync(bm.MessageId);
      }

      private QueueMessage ProcessAndConvert(Message bm)
      {
         QueueMessage qm = Converter.ToQueueMessage(bm);
         if (_peekLock) _messageIdToBrokeredMessage[qm.Id] = bm;
         return qm;
      }

      /// <summary>
      /// Call at the end when done with the message.
      /// </summary>
      public async Task ConfirmMessagesAsync(IReadOnlyCollection<QueueMessage> messages, CancellationToken cancellationToken)
      {
         if (!_peekLock)
            return;

         await Task.WhenAll(messages.Select(m => ConfirmAsync(m)));
      }

      private async Task ConfirmAsync(QueueMessage message)
      {
         //delete the message and get the deleted element, very nice method!
         if (!_messageIdToBrokeredMessage.TryRemove(message.Id, out Message bm))
            return;

         await _client.CompleteAsync(bm.SystemProperties.LockToken);
      }

      /// <summary>
      /// Starts message pump with AutoComplete = false and the defined <see cref="AzureReceiverOptions"/>.
      /// </summary>
      public Task StartMessagePumpAsync(Func<IReadOnlyCollection<QueueMessage>, Task> onMessageAsync, int maxBatchSize = 1, CancellationToken cancellationToken = default(CancellationToken))
      {
         if (onMessageAsync == null) throw new ArgumentNullException(nameof(onMessageAsync));

         var options = new MessageHandlerOptions(_azureRegisterMessageOptions.ExceptionReceivedHandler ?? ExceptionReceiverHandler)
         {
            AutoComplete = false,
            MaxAutoRenewDuration = _azureRegisterMessageOptions.MaxAutoRenewDuration,
            MaxConcurrentCalls = _azureRegisterMessageOptions.MaxConcurrentCalls
         };

         _client.PrefetchCount = maxBatchSize;

         _client.RegisterMessageHandler(
            async (message, token) =>
            {
               QueueMessage qm = Converter.ToQueueMessage(message);
               _messageIdToBrokeredMessage[qm.Id] = message;
               await onMessageAsync(new[] { qm });
            },
            options);

         return Task.FromResult(true);
      }

      private Task ExceptionReceiverHandler(ExceptionReceivedEventArgs args)
      {
         return Task.FromResult(true);
      }

      /// <summary>
      /// Stops message pump if started
      /// </summary>
      public void Dispose()
      {
         _client.CloseAsync().Wait();  //this also stops the message pump
      }

      /// <summary>
      /// Empty transaction
      /// </summary>
      public Task<ITransaction> OpenTransactionAsync()
      {
         return Task.FromResult(EmptyTransaction.Instance);
      }
   }
}
