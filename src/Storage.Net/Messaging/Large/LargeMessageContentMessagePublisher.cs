using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Storage.Net.Blob;

namespace Storage.Net.Messaging.Large
{
   class LargeMessageContentMessagePublisher : IMessagePublisher
   {
      private readonly IMessagePublisher _parentPublisher;
      private readonly IBlobStorage _offloadStorage;
      private readonly long _minSizeLarge;
      private readonly bool _keepPublisherOpen;
      private readonly Func<QueueMessage, string> _blobPathGenerator;

      public LargeMessageContentMessagePublisher(
         IMessagePublisher parentPublisher,
         IBlobStorage offloadStorage,
         long minSizeLarge,
         Func<QueueMessage, string> blobPathGenerator = null,
         bool keepPublisherOpen = false)
      {
         _parentPublisher = parentPublisher ?? throw new ArgumentNullException(nameof(parentPublisher));
         _offloadStorage = offloadStorage ?? throw new ArgumentNullException(nameof(offloadStorage));
         _minSizeLarge = minSizeLarge;
         _blobPathGenerator = blobPathGenerator ?? GenerateBlobPath;
         _keepPublisherOpen = keepPublisherOpen;
      }

      public async Task PutMessagesAsync(IReadOnlyCollection<QueueMessage> messages, CancellationToken cancellationToken = default)
      {
         if (messages == null) return;

         await Task.WhenAll(messages.Select(m => PutMessageAsync(m, cancellationToken)));
      }

      private async Task PutMessageAsync(QueueMessage message, CancellationToken cancellationToken)
      {
         if(message?.Content.Length > _minSizeLarge)
         {
            AddBlobId(message, out string id);

            await _offloadStorage.WriteAsync(id, message.Content, false, cancellationToken);

            message.Content = null; //delete content
         }

         await _parentPublisher.PutMessageAsync(message);
      }

      private void AddBlobId(QueueMessage message, out string id)
      {
         id = _blobPathGenerator(message);

         message.Properties[QueueMessage.LargeMessageContentHeaderName] = id;
      }

      private string GenerateBlobPath(QueueMessage message)
      {
         return StoragePath.Combine("message", Guid.NewGuid().ToString());
      }

      public void Dispose()
      {
         if (!_keepPublisherOpen)
         {
            _parentPublisher.Dispose();
         }
      }

   }
}
