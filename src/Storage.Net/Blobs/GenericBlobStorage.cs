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
      public virtual async Task<IReadOnlyCollection<Blob>> ListAsync(ListOptions options = null, CancellationToken cancellationToken = default)
      {
         var result = new List<Blob>();
         if(options == null) options = new ListOptions();

         await ListStepAsync(options.FolderPath, options, result, cancellationToken).ConfigureAwait(false);

         if(options.MaxResults != null && result.Count > options.MaxResults.Value)
         {
            result = result.Take(options.MaxResults.Value).ToList();
         }

         return result;
      }

      private async Task ListStepAsync(string path, ListOptions options, List<Blob> container, CancellationToken cancellationToken)
      {
         IReadOnlyCollection<Blob> chunk = await ListAtAsync(path, options, cancellationToken).ConfigureAwait(false);

         if(options.BrowseFilter != null)
         {
            container.AddRange(chunk.Where(b => options.BrowseFilter(b)));
         }
         else
         {
            container.AddRange(chunk);
         }

         if(options.MaxResults != null && container.Count >= options.MaxResults.Value)
            return;

         if(options.Recurse)
         {
            await Task.WhenAll(
               chunk.Where(c => c.IsFolder).ToList()
               .Select(c => ListStepAsync(c.FullPath, options, container, cancellationToken))).ConfigureAwait(false);
         }
      }

      /// <summary>
      /// 
      /// </summary>
      protected virtual Task<IReadOnlyCollection<Blob>> ListAtAsync(string path, ListOptions options, CancellationToken cancellationToken)
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
      public async Task<IReadOnlyCollection<Blob>> GetBlobsAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default)
      {
         return await Task.WhenAll(fullPaths.Select(fp => GetBlobAsync(fp, cancellationToken))).ConfigureAwait(false);
      }

      /// <summary>
      /// 
      /// </summary>
      protected virtual Task<Blob> GetBlobAsync(string fullPath, CancellationToken cancellationToken) => throw new NotSupportedException();

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
