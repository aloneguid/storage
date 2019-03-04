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
   class AzureServiceBusQueueReceiver : IMessageReceiver
   {
      //https://github.com/Azure/azure-service-bus/blob/master/samples/DotNet/Microsoft.Azure.ServiceBus/ReceiveSample/readme.md

      private static readonly TimeSpan AutoRenewDuration = TimeSpan.FromMinutes(1);

      private readonly QueueClient _client;
      private readonly bool _peekLock;
      private readonly MessageHandlerOptions _messageHandlerOptions;
      private readonly ConcurrentDictionary<string, Message> _messageIdToBrokeredMessage = new ConcurrentDictionary<string, Message>();

      /// <summary>
      /// Creates an instance of Azure Service Bus receiver with connection
      /// </summary>
      /// <param name="connectionString">Service Bus connection string</param>
      /// <param name="queueName">Queue name in Service Bus</param>
      /// <param name="peekLock">When true listens in PeekLock mode, otherwise ReceiveAndDelete</param>
      /// <param name="messageHandlerOptions"></param>
      public AzureServiceBusQueueReceiver(string connectionString, string queueName, bool peekLock = true,
         MessageHandlerOptions messageHandlerOptions = null)
      {
         _client = new QueueClient(connectionString, queueName, peekLock ? ReceiveMode.PeekLock : ReceiveMode.ReceiveAndDelete);
         _peekLock = peekLock;
         _messageHandlerOptions = messageHandlerOptions;
      }

      /// <summary>
      /// See interface
      /// </summary>
      /// <returns></returns>
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
         if(_peekLock) _messageIdToBrokeredMessage[qm.Id] = bm;
         return qm;
      }

      /// <summary>
      /// Call at the end when done with the message.
      /// </summary>
      public async Task ConfirmMessagesAsync(IReadOnlyCollection<QueueMessage> messages, CancellationToken cancellationToken)
      {
         if(!_peekLock) return;

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
      /// Starts message pump with AutoComplete = false, 1 minute session renewal and 1 concurrent call.
      /// </summary>
      public Task StartMessagePumpAsync(Func<IReadOnlyCollection<QueueMessage>, Task> onMessage, int maxBatchSize, CancellationToken cancellationToken)
      {
         if (onMessage == null) throw new ArgumentNullException(nameof(onMessage));

         MessageHandlerOptions options = _messageHandlerOptions ?? new MessageHandlerOptions(ExceptionReceiverHandler)
         {
            AutoComplete = false,
            MaxAutoRenewDuration = AutoRenewDuration,
            MaxConcurrentCalls = 1
         };

         _client.PrefetchCount = maxBatchSize;

         _client.RegisterMessageHandler(
            async (message, token) =>
            {
               QueueMessage qm = Converter.ToQueueMessage(message);
               _messageIdToBrokeredMessage[qm.Id] = message;
               await onMessage(new[] { qm });
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
      /// Transactions are not supported, returns empty transation
      /// </summary>
      public Task<ITransaction> OpenTransactionAsync()
      {
         return Task.FromResult(EmptyTransaction.Instance);
      }
   }
}
