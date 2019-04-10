using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Storage.Net.Messaging;
using IMessageReceiver = Storage.Net.Messaging.IMessageReceiver;

namespace Storage.Net.Microsoft.Azure.ServiceBus.Messaging
{
   abstract class AzureServiceBusReceiver : IMessageReceiver
   {
      protected readonly ConcurrentDictionary<string, Message> _messageIdToBrokeredMessage = new ConcurrentDictionary<string, Message>();
      protected readonly IReceiverClient _receiverClient;
      protected readonly MessageHandlerOptions _messageHandlerOptions;
      protected readonly bool _autoComplete;

      public AzureServiceBusReceiver(IReceiverClient receiverClient, MessageHandlerOptions messageHandlerOptions)
      {
         _receiverClient = receiverClient ?? throw new ArgumentNullException(nameof(receiverClient));

         _messageHandlerOptions = messageHandlerOptions ??
            new MessageHandlerOptions(DefaultExceptionReceiverHandler)
            {
               AutoComplete = false,
               MaxAutoRenewDuration = TimeSpan.FromMinutes(1),
               MaxConcurrentCalls = 1
            };

         _autoComplete = _messageHandlerOptions.AutoComplete;
      }

      private static Task DefaultExceptionReceiverHandler(ExceptionReceivedEventArgs args)
      {
         if(args?.Exception is OperationCanceledException)
         {
            // operation cancelled, ignore
         }

         //extra handling code
         return Task.FromResult(true);
      }

      public async Task ConfirmMessagesAsync(IReadOnlyCollection<QueueMessage> messages, CancellationToken cancellationToken = default)
      {
         if(_autoComplete)
            return;

         await Task.WhenAll(messages.Select(m => ConfirmAsync(m))).ConfigureAwait(false);
      }

      private async Task ConfirmAsync(QueueMessage message)
      {
         //delete the message and get the deleted element, very nice method!
         if(!_messageIdToBrokeredMessage.TryRemove(message.Id, out Message bm))
            return;

         await _receiverClient.CompleteAsync(bm.SystemProperties.LockToken).ConfigureAwait(false);
      }

      public Task<int> GetMessageCountAsync() => throw new NotSupportedException();


      public async Task DeadLetterAsync(QueueMessage message, string reason, string errorDescription, CancellationToken cancellationToken = default)
      {
         if(_autoComplete)
            return;

         if(!_messageIdToBrokeredMessage.TryRemove(message.Id, out Message bm))
            return;

         await _receiverClient.DeadLetterAsync(bm.MessageId).ConfigureAwait(false);
      }

      public Task<ITransaction> OpenTransactionAsync()
      {
         return Task.FromResult(EmptyTransaction.Instance);
      }

      public Task StartMessagePumpAsync(Func<IReadOnlyCollection<QueueMessage>, Task> onMessageAsync, int maxBatchSize = 1, CancellationToken cancellationToken = default)
      {
         if(onMessageAsync == null)
            throw new ArgumentNullException(nameof(onMessageAsync));

         _receiverClient.PrefetchCount = maxBatchSize;

         _receiverClient.RegisterMessageHandler(
            async (message, token) =>
            {
               QueueMessage qm = Converter.ToQueueMessage(message);
               if(!_autoComplete)
                  _messageIdToBrokeredMessage[qm.Id] = message;
               await onMessageAsync(new[] { qm });
            },
            _messageHandlerOptions);

         return Task.FromResult(true);
      }

      public void Dispose()
      {
         _receiverClient.CloseAsync();
      }

   }
}
