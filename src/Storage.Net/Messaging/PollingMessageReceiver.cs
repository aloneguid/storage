using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NetBox.Extensions;

namespace Storage.Net.Messaging
{
   /// <summary>
   /// Base class for implementing a polling message receiver for those providers that do not support polling natively.
   /// </summary>
   public abstract class PollingMessageReceiver : IMessageReceiver
   {
      private readonly int _pollIntervalSeconds;

      /// <summary>
      /// 
      /// </summary>
      /// <param name="pollIntervalSeconds">Poll interval, defaults to one second</param>
      protected PollingMessageReceiver(int pollIntervalSeconds = 1)
      {
         _pollIntervalSeconds = pollIntervalSeconds;
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
      public async Task StartMessagePumpAsync(Func<IReadOnlyCollection<QueueMessage>, Task> onMessageAsync, int maxBatchSize = 1, CancellationToken cancellationToken = default)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
      {
         if (onMessageAsync == null) throw new ArgumentNullException(nameof(onMessageAsync));

         PollTasksAsync(onMessageAsync, maxBatchSize, cancellationToken).Forget();
      }

      private async Task PollTasksAsync(Func<IReadOnlyCollection<QueueMessage>, Task> callback, int maxBatchSize, CancellationToken cancellationToken)
      {
         IReadOnlyCollection<QueueMessage> messages = await ReceiveMessagesAsync(maxBatchSize, cancellationToken).ConfigureAwait(false);
         while (messages != null && messages.Count > 0)
         {
            await callback(messages);

            messages = await ReceiveMessagesAsync(maxBatchSize, cancellationToken).ConfigureAwait(false);
         }

         await Task.Delay(TimeSpan.FromSeconds(_pollIntervalSeconds), cancellationToken).ContinueWith(async (t) =>
         {
            await PollTasksAsync(callback, maxBatchSize, cancellationToken).ConfigureAwait(false);
         }).ConfigureAwait(false);
      }

      /// <summary>
      /// See interface
      /// </summary>
      protected abstract Task<IReadOnlyCollection<QueueMessage>> ReceiveMessagesAsync(int maxBatchSize, CancellationToken cancellationToken);
   }
}
