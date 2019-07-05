using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Storage.Net.Messaging;
using IMessageReceiver = Storage.Net.Messaging.IMessageReceiver;
using ISBMessageReceiver = Microsoft.Azure.ServiceBus.Core.IMessageReceiver;

namespace Storage.Net.Microsoft.Azure.ServiceBus.Messaging
{
   abstract class AzureServiceBusReceiver : IMessageReceiver
   {
      protected readonly ConcurrentDictionary<string, Message> _messageIdToBrokeredMessage = new ConcurrentDictionary<string, Message>();
      protected readonly IReceiverClient _receiverClient;
      //message received is only used for "advanced" operations not available in IReceiverClient. If you can not use it please do.
      private readonly ISBMessageReceiver _messageReceiver;

      protected readonly MessageHandlerOptions _messageHandlerOptions;
      protected readonly bool _autoComplete;

      private CancellationTokenSource _cancellationTokenSource;

      public AzureServiceBusReceiver(IReceiverClient receiverClient, ISBMessageReceiver messageReceiver, MessageHandlerOptions messageHandlerOptions)
      {
         _receiverClient = receiverClient ?? throw new ArgumentNullException(nameof(receiverClient));
         _messageReceiver = messageReceiver ?? throw new ArgumentNullException(nameof(messageReceiver));
         _messageHandlerOptions = messageHandlerOptions ??
            new MessageHandlerOptions(DefaultExceptionReceiverHandler)
            {
               AutoComplete = false,


               /*
                * In fact, what the property actually means is the maximum about of time they lock renewal will happen for internally on the subscription client.
                * So if you set this to 24 hours e.g. Timespan.FromHours(24) and your processing was to take 12 hours, it would be renewed. However, if you set
                * this to 12 hours using Timespan.FromHours(12) and your code ran for 24, when you went to complete the message it would give a lockLost exception
                * (as I was getting above over shorter intervals!).
                * 
                * in fact, Microsoft's implementation runs a background task that periodically renews the message lock until it expires.
                */
               MaxAutoRenewDuration = TimeSpan.FromMinutes(10), //should be in fact called "max processing time"
               MaxConcurrentCalls = 2
            };

         _autoComplete = _messageHandlerOptions.AutoComplete;

         //note: we can't use management SDK as it requires high priviledged SP in Azure
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

      public async Task KeepAliveAsync(QueueMessage message, TimeSpan? timeToLive = null, CancellationToken cancellationToken = default)
      {
         if(_autoComplete)
            return;

         if(!_messageIdToBrokeredMessage.TryGetValue(message.Id, out Message bm))
            return;

         await _messageReceiver.RenewLockAsync(bm).ConfigureAwait(false);
      }

      public Task<ITransaction> OpenTransactionAsync()
      {
         return Task.FromResult(EmptyTransaction.Instance);
      }

      public async Task ListenAsync(
         Func<IReadOnlyCollection<QueueMessage>, CancellationToken, Task> onMessageAsync,
         int maxBatchSize = 1,
         CancellationToken cancellationToken = default)
      {
         if(onMessageAsync == null)
            throw new ArgumentNullException(nameof(onMessageAsync));

         if(_cancellationTokenSource != null)
         {
            _cancellationTokenSource.Dispose();
         }

         _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

         _receiverClient.PrefetchCount = maxBatchSize;
         _receiverClient.RegisterMessageHandler(
            async (message, token) =>
            {
               var queueMessage = Converter.ToQueueMessage(message);

               if(!_autoComplete)
                  _messageIdToBrokeredMessage[queueMessage.Id] = message;

               using(var linkedSource = CancellationTokenSource.CreateLinkedTokenSource(token, _cancellationTokenSource.Token))
               {
                  await onMessageAsync(new[] { queueMessage }, linkedSource.Token).ConfigureAwait(false);
               }
            },
            _messageHandlerOptions);

         var source = new TaskCompletionSource<object>();
         _cancellationTokenSource.Token.Register(() => source.SetResult(null));
         await source.Task.ConfigureAwait(false);
      }

      public void Dispose()
      {
         if(_cancellationTokenSource != null)
         {
            _cancellationTokenSource.Cancel();
            _cancellationTokenSource.Dispose();
            _cancellationTokenSource = null;
         }

         _receiverClient.CloseAsync().GetAwaiter().GetResult();
      }

      protected static MessageReceiver CreateMessageReceiver(string connectionString, string entityName, bool peekLock)
      {
         ReceiveMode mode = peekLock ? ReceiveMode.PeekLock : ReceiveMode.ReceiveAndDelete;
         return new MessageReceiver(connectionString, entityName, mode);
      }
   }
}
