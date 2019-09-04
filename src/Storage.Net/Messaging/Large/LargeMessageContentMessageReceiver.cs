using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Storage.Net.Blobs;

namespace Storage.Net.Messaging.Large
{
   class LargeMessageContentMessageReceiver : IMessageReceiver
   {
      private readonly IMessageReceiver _parentReceiver;
      private readonly IBlobStorage _offloadStorage;

      public LargeMessageContentMessageReceiver(IMessageReceiver parentReceiver, IBlobStorage offloadStorage)
      {
         _parentReceiver = parentReceiver;
         _offloadStorage = offloadStorage;
      }

      public async Task ConfirmMessagesAsync(IReadOnlyCollection<QueueMessage> messages, CancellationToken cancellationToken = default)
      {
         await _parentReceiver.ConfirmMessagesAsync(messages, cancellationToken);

         foreach(QueueMessage message in messages)
         {
            await DeleteBlobAsync(message);
         }
      }

      public async Task DeadLetterAsync(QueueMessage message, string reason, string errorDescription, CancellationToken cancellationToken = default)
      {
         await _parentReceiver.DeadLetterAsync(message, reason, errorDescription, cancellationToken);

         await DeleteBlobAsync(message);
      }

      private async Task DeleteBlobAsync(QueueMessage message)
      {
         if (!message.Properties.TryGetValue(QueueMessage.LargeMessageContentHeaderName, out string fileId)) return;

         message.Properties.Remove(QueueMessage.LargeMessageContentHeaderName);

         await _offloadStorage.DeleteAsync(fileId);
      }

      public void Dispose()
      {
         _parentReceiver.Dispose();
      }

      public Task<int> GetMessageCountAsync() => _parentReceiver.GetMessageCountAsync();

      public Task<ITransaction> OpenTransactionAsync() => _parentReceiver.OpenTransactionAsync();

      public Task StartMessagePumpAsync(Func<IReadOnlyCollection<QueueMessage>, CancellationToken, Task> onMessageAsync, int maxBatchSize = 1, CancellationToken cancellationToken = default)
      {
         return _parentReceiver.StartMessagePumpAsync(
            (mms, ct) => DownloadingMessagePumpAsync(mms, onMessageAsync, ct),
            maxBatchSize, cancellationToken);
      }

      private async Task DownloadingMessagePumpAsync(IReadOnlyCollection<QueueMessage> messages,
         Func<IReadOnlyCollection<QueueMessage>, CancellationToken, Task> onParentMessagesAsync,
         CancellationToken cancellationToken)
      {
         //process messages to download external content
         foreach(QueueMessage message in messages)
         {
            if (!message.Properties.TryGetValue(QueueMessage.LargeMessageContentHeaderName, out string fileId)) continue;

            message.Content = await _offloadStorage.ReadBytesAsync(fileId, cancellationToken);
         }

         //now that messages are augmented pass them to parent
         await onParentMessagesAsync(messages, cancellationToken);
      }

      public Task KeepAliveAsync(QueueMessage message, TimeSpan? timeToLive = null, CancellationToken cancellationToken = default) =>
         _parentReceiver.KeepAliveAsync(message, timeToLive, cancellationToken);
      public Task<IReadOnlyCollection<QueueMessage>> PeekMessagesAsync(int maxMessages, CancellationToken cancellationToken = default) => throw new NotSupportedException();
   }
}
