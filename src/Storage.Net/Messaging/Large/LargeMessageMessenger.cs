using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Storage.Net.Blobs;

namespace Storage.Net.Messaging.Large
{
   class LargeMessageMessenger : IMessenger
   {
      private readonly IMessenger _parentPublisher;
      private readonly IBlobStorage _offloadStorage;
      private readonly long _minSizeLarge;
      private readonly bool _keepPublisherOpen;
      private readonly Func<QueueMessage, string> _blobPathGenerator;

      public LargeMessageMessenger(
         IMessenger parentPublisher,
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

      private void AddBlobId(QueueMessage message, out string id)
      {
         id = _blobPathGenerator(message);

         message.Properties[QueueMessage.LargeMessageContentHeaderName] = id;
      }

      private string GenerateBlobPath(QueueMessage message)
      {
         return StoragePath.Combine("message", Guid.NewGuid().ToString());
      }

      private async Task SendAsync(string channelName, QueueMessage message, CancellationToken cancellationToken)
      {
         if(message?.Content.Length > _minSizeLarge)
         {
            AddBlobId(message, out string id);

            await _offloadStorage.WriteAsync(id, message.Content, false, cancellationToken).ConfigureAwait(false);

            message.Content = null; //delete content
         }

         await _parentPublisher.SendAsync(channelName, new[] { message }).ConfigureAwait(false);
      }


      #region [ IMessenger ]

      public Task<IReadOnlyCollection<string>> ListChannelsAsync(CancellationToken cancellationToken = default) => throw new NotImplementedException();
      public Task<long> GetMessageCountAsync(string channelName, CancellationToken cancellationToken = default) => throw new NotImplementedException();
      public async Task SendAsync(string channelName, IEnumerable<QueueMessage> messages, CancellationToken cancellationToken = default)
      {
         await Task.WhenAll(messages.Select(m => SendAsync(channelName, m, cancellationToken))).ConfigureAwait(false);
      }

      public Task<IReadOnlyCollection<QueueMessage>> ReceiveAsync(string channelName, int count = 100, TimeSpan? visibility = null, CancellationToken cancellationToken = default) => throw new NotImplementedException();
      public Task<IReadOnlyCollection<QueueMessage>> PeekAsync(string channelName, int count = 100, CancellationToken cancellationToken = default) => throw new NotImplementedException();

      public void Dispose()
      {
         if(!_keepPublisherOpen)
         {
            _parentPublisher.Dispose();
         }
      }

      public Task DeleteChannelsAsync(IEnumerable<string> channelNames, CancellationToken cancellationToken = default) => throw new NotImplementedException();
      public Task CreateChannelsAsync(IEnumerable<string> channelNames, CancellationToken cancellationToken = default) => throw new NotImplementedException();
      public Task DeleteAsync(string channelName, IEnumerable<QueueMessage> messages, CancellationToken cancellationToken = default) => throw new NotImplementedException();
      public Task StartMessageProcessorAsync(string channelName, IMessageProcessor messageProcessor) => throw new NotImplementedException();

      #endregion
   }
}
