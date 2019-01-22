using System;
using System.Collections.Generic;
using System.Text;
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
      /// <summary>
      /// See interface
      /// </summary>
      public virtual Task<int> GetMessageCountAsync()
      {
         throw new NotSupportedException();
      }

      /// <summary>
      /// See interface
      /// </summary>
      public virtual Task ConfirmMessagesAsync(IReadOnlyCollection<QueueMessage> messages, CancellationToken cancellationToken = default)
      {
         throw new NotSupportedException();
      }

      /// <summary>
      /// See interface
      /// </summary>
      public virtual Task DeadLetterAsync(QueueMessage message, string reason, string errorDescription, CancellationToken cancellationToken = default)
      {
         throw new NotSupportedException();
      }

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

         PollTasks(onMessageAsync, maxBatchSize, cancellationToken).Forget();
      }

      private async Task PollTasks(Func<IReadOnlyCollection<QueueMessage>, Task> callback, int maxBatchSize, CancellationToken cancellationToken)
      {
         IReadOnlyCollection<QueueMessage> messages = await ReceiveMessagesAsync(maxBatchSize, cancellationToken);
         while (messages != null && messages.Count > 0)
         {
            await callback(messages);

            messages = await ReceiveMessagesAsync(maxBatchSize, cancellationToken);
         }

         await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken).ContinueWith(async (t) =>
         {
            await PollTasks(callback, maxBatchSize, cancellationToken);
         });
      }

      /// <summary>
      /// See interface
      /// </summary>
      protected abstract Task<IReadOnlyCollection<QueueMessage>> ReceiveMessagesAsync(int maxBatchSize, CancellationToken cancellationToken);
   }
}
