using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NetBox.Extensions;
using Storage.Net.Messaging.Polling;

namespace Storage.Net.Messaging
{
   /// <summary>
   /// Base class for implementing a polling message receiver for those providers that do not support polling natively.
   /// </summary>
   public abstract class PollingMessageReceiver : IMessageReceiver
   {
      private readonly IPollingPolicy _pollingPolicy;

      /// <summary>
      /// 
      /// </summary>
      protected PollingMessageReceiver()
      {
         _pollingPolicy = new ExponentialBackoffPollingPolicy(TimeSpan.FromMilliseconds(100), TimeSpan.FromMinutes(15));
      }

      /// <summary>
      /// See interface
      /// </summary>
      public abstract Task<int> GetMessageCountAsync();

      /// <summary>
      /// See interface
      /// </summary>
      public abstract Task ConfirmMessagesAsync(IReadOnlyCollection<QueueMessage> messages, CancellationToken cancellationToken = default);

      /// <summary>
      /// See interface
      /// </summary>
      public abstract Task DeadLetterAsync(QueueMessage message, string reason, string errorDescription, CancellationToken cancellationToken = default);

      /// <summary>
      /// See interface
      /// </summary>
      public virtual void Dispose()
      {
      }

      /// <summary>
      /// See interface
      /// </summary>
      public virtual Task<ITransaction> OpenTransactionAsync()
      {
         return Task.FromResult(EmptyTransaction.Instance);
      }

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
                              /// <summary>
                              /// See interface
                              /// </summary>
      public Task StartMessagePumpAsync(Func<IReadOnlyCollection<QueueMessage>, Task> onMessageAsync, int maxBatchSize = 1, CancellationToken cancellationToken = default)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
      {
         if (onMessageAsync == null) throw new ArgumentNullException(nameof(onMessageAsync));

         Task.Factory.StartNew(() => PollTasksAsync(onMessageAsync, maxBatchSize, cancellationToken), TaskCreationOptions.LongRunning);

         return Task.FromResult(true);
      }

      private async Task PollTasksAsync(Func<IReadOnlyCollection<QueueMessage>, Task> callback, int maxBatchSize, CancellationToken cancellationToken)
      {
         try
         {
            IReadOnlyCollection<QueueMessage> messages = await ReceiveMessagesSafeAsync(maxBatchSize, cancellationToken).ConfigureAwait(false);
            while(messages != null && messages.Count > 0)
            {
               await callback(messages);

               messages = await ReceiveMessagesSafeAsync(maxBatchSize, cancellationToken).ConfigureAwait(false);

               _pollingPolicy.Reset();
            }

            await Task.Delay(_pollingPolicy.GetNextDelay(), cancellationToken).ContinueWith(async (t) =>
            {
               await PollTasksAsync(callback, maxBatchSize, cancellationToken).ConfigureAwait(false);
            }).ConfigureAwait(false);
         }
         catch(TaskCanceledException)
         {
            //terminate polling as there is nothing to do when task is cancelled
            return;
         }
         catch(Exception ex)
         {
            Console.WriteLine(ex.ToString());
         }
      }

      private async Task<IReadOnlyCollection<QueueMessage>> ReceiveMessagesSafeAsync(int maxBatchSize, CancellationToken cancellationToken)
      {
         try
         {
            IReadOnlyCollection<QueueMessage> messages = await ReceiveMessagesAsync(maxBatchSize, cancellationToken).ConfigureAwait(false);

            return messages;
         }
         catch(TaskCanceledException)
         {
            throw;   //bubble it up
         }
         catch(Exception ex)
         {
            Console.WriteLine(ex.ToString());
         }

         return null;
      }

      /// <summary>
      /// See interface
      /// </summary>
      protected abstract Task<IReadOnlyCollection<QueueMessage>> ReceiveMessagesAsync(int maxBatchSize, CancellationToken cancellationToken);
   }
}
