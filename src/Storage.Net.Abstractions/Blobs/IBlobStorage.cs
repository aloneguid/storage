using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Storage.Net.Blobs
{
   /// <summary>
   /// Slim interface providing access to blob storage.
   /// </summary>
   public interface IBlobStorage : IDisposable
   {
      /// <summary>
      /// Returns the list of available blobs
      /// </summary>
      /// <param name="options"></param>
      /// <param name="cancellationToken"></param>
      /// <returns>List of blob IDs</returns>
      Task<IReadOnlyCollection<Blob>> ListAsync(
         ListOptions options = null,
         CancellationToken cancellationToken = default);

      /// <summary>
      /// Creates a new blob and opens a writeable stream for it. If the blob already exists it will be
      /// overwritten.
      /// </summary>
      /// <param name="fullPath">Blob metadata</param>
      /// <param name="cancellationToken"></param>
      /// <param name="append">When true, appends to the file instead of writing a new one.</param>
      /// <returns>Writeable stream</returns>
      /// <exception cref="ArgumentNullException">Thrown when any parameter is null</exception>
      /// <exception cref="ArgumentException">Thrown when ID is too long. Long IDs are the ones longer than 50 characters.</exception>
      Task<Stream> OpenWriteAsync(string fullPath, bool append = false, CancellationToken cancellationToken = default);

      /// <summary>
      /// Opens the blob stream to read.
      /// </summary>
      /// <param name="fullPath">Blob's full path</param>
      /// <param name="cancellationToken"></param>
      /// <returns>Stream in an open state, or null if blob doesn't exist by this ID. It is your responsibility to close and dispose this
      /// stream after use.</returns>
      /// <exception cref="ArgumentNullException">Thrown when any parameter is null</exception>
      /// <exception cref="ArgumentException">Thrown when ID is too long. Long IDs are the ones longer than 50 characters.</exception>
      Task<Stream> OpenReadAsync(string fullPath, CancellationToken cancellationToken = default);

      /// <summary>
      /// Deletes an object by it's full path.
      /// </summary>
      /// <param name="fullPaths">Path to delete. If this path points to a folder, the folder is deleted recursively.</param>
      /// <param name="cancellationToken"></param>
      /// <exception cref="ArgumentNullException">Thrown when ID is null.</exception>
      /// <exception cref="ArgumentException">Thrown when ID is too long. Long IDs are the ones longer than 50 characters.</exception>
      Task DeleteAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default);

      /// <summary>
      /// Checksi if blobs exists in the storage
      /// </summary>
      /// <param name="fullPaths">List of paths to blobs</param>
      /// <param name="cancellationToken"></param>
      /// <returns>List of results of true and false indicating existence</returns>
      Task<IReadOnlyCollection<bool>> ExistsAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default);

      /// <summary>
      /// Gets blob information which is useful for retreiving blob metadata
      /// </summary>
      Task<IReadOnlyCollection<Blob>> GetBlobsAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default);

      /// <summary>
      /// Set blob information which is useful for setting blob attributes (user metadata etc.)
      /// </summary>
      Task SetBlobsAsync(IEnumerable<Blob> blobs, CancellationToken cancellationToken = default);

      /// <summary>
      /// Starts a new transaction
      /// </summary>
      /// <returns></returns>
      Task<ITransaction> OpenTransactionAsync();
   }
}
