using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Storage.Net.Blobs
{
   /// <summary>
   /// Provides the most generic form of the blob storage implementation
   /// </summary>
   public abstract class GenericBlobStorage : IBlobStorage
   {
      /// <summary>
      /// Lists blobs
      /// </summary>
      public virtual Task<IReadOnlyCollection<Blob>> ListAsync(ListOptions options = null, CancellationToken cancellationToken = default)
      {
         throw new NotSupportedException();
      }

      /// <summary>
      /// Delete all blobs
      /// </summary>
      public virtual Task DeleteAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default)
      {
         return Task.WhenAll(fullPaths.Select(fp => DeleteSingleAsync(fp, cancellationToken)));
      }

      /// <summary>
      /// Deletes one
      /// </summary>
      protected virtual Task DeleteSingleAsync(string fullPath, CancellationToken cancellationToken)
      {
         throw new NotSupportedException();
      }

      /// <summary>
      /// 
      /// </summary>
      public virtual async Task<IReadOnlyCollection<bool>> ExistsAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default)
      {
         return await Task.WhenAll(fullPaths.Select(fp => ExistsAsync(fp, cancellationToken))).ConfigureAwait(false);
      }

      /// <summary>
      /// 
      /// </summary>
      protected virtual Task<bool> ExistsAsync(string fullPath, CancellationToken cancellationToken)
      {
         throw new NotSupportedException();
      }

      /// <summary>
      /// 
      /// </summary>
      public Task<IReadOnlyCollection<Blob>> GetBlobsAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default) => throw new NotSupportedException();

      /// <summary>
      /// 
      /// </summary>
      public virtual Task<Stream> OpenReadAsync(string fullPath, CancellationToken cancellationToken = default)
      {
         throw new NotSupportedException();
      }

      /// <summary>
      /// 
      /// </summary>
      public Task<ITransaction> OpenTransactionAsync() => throw new NotSupportedException();

      /// <summary>
      /// 
      /// </summary>
      public virtual Task<Stream> OpenWriteAsync(string fullPath, bool append = false, CancellationToken cancellationToken = default)
      {
         throw new NotSupportedException();
      }

      /// <summary>
      /// 
      /// </summary>
      public Task SetBlobsAsync(IEnumerable<Blob> blobs, CancellationToken cancellationToken = default) => throw new NotSupportedException();

      /// <summary>
      /// Dispose any unused resources
      /// </summary>
      public virtual void Dispose()
      {

      }

   }
}
