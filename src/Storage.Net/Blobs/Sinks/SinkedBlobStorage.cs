using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Storage.Net.Blobs.Sinks
{
   class SinkedBlobStorage : IBlobStorage
   {
      private readonly IBlobStorage _parent;
      private readonly ITransformSink[] _sinks;

      public SinkedBlobStorage(IBlobStorage blobStorage, params ITransformSink[] sinks)
      {
         if(sinks is null)
            throw new ArgumentNullException(nameof(sinks));

         _parent = blobStorage ?? throw new ArgumentNullException(nameof(blobStorage));
         _sinks = sinks;
      }

      public Task DeleteAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default)
      {
         return _parent.DeleteAsync(fullPaths, cancellationToken);
      }

      public void Dispose() => _parent.Dispose();

      public Task<IReadOnlyCollection<bool>> ExistsAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default)
      {
         return _parent.ExistsAsync(fullPaths, cancellationToken);
      }

      public Task<IReadOnlyCollection<Blob>> GetBlobsAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default)
      {
         return _parent.GetBlobsAsync(fullPaths, cancellationToken);
      }

      public Task<IReadOnlyCollection<Blob>> ListAsync(ListOptions options = null, CancellationToken cancellationToken = default)
      {
         return _parent.ListAsync(options, cancellationToken);
      }

      public Task<ITransaction> OpenTransactionAsync() => _parent.OpenTransactionAsync();

      public Task SetBlobsAsync(IEnumerable<Blob> blobs, CancellationToken cancellationToken = default) => _parent.SetBlobsAsync(blobs, cancellationToken);

      public async Task<Stream> OpenReadAsync(string fullPath, CancellationToken cancellationToken = default)
      {
         //chain streams
         Stream readStream = await _parent.OpenReadAsync(fullPath, cancellationToken).ConfigureAwait(false);

         if(readStream == null)
            return null;

         foreach(ITransformSink sink in _sinks)
         {
            readStream = sink.OpenReadStream(fullPath, readStream);
         }

         return readStream;
      }

      public async Task WriteAsync(
         string fullPath, Stream dataSourceStream,
         bool append = false,
         CancellationToken cancellationToken = default)
      {
         if(dataSourceStream == null)
            return;

         using(var source = new SinkedStream(dataSourceStream, fullPath, _sinks))
         {
            await _parent.WriteAsync(fullPath, source, append, cancellationToken).ConfigureAwait(false);
         }
      }
   }
}
