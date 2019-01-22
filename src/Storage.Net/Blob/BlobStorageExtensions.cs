using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NetBox.Extensions;

namespace Storage.Net.Blob
{
   /// <summary>
   /// Blob storage on steroids. Takes in <see cref="IBlobStorage"/> and adds a lot of extra useful operations on top we as
   /// normal people use every day.
   /// </summary>
   public static class BlobStorageExtensions
   {
      private const int BufferSize = 81920;

      #region [ List Helpers ]

      /// <summary>
      /// Returns the list of available files, excluding folders.
      /// </summary>
      /// <param name="provider"></param>
      /// <param name="options"></param>
      /// <param name="cancellationToken"></param>
      /// <returns>List of blob IDs</returns>
      public static async Task<IReadOnlyCollection<BlobId>> ListFilesAsync(this IBlobStorage provider,
         ListOptions options,
         CancellationToken cancellationToken = default)
      {
         IEnumerable<BlobId> all = await provider.ListAsync(options, cancellationToken);

         return all.Where(i => i.Kind == BlobItemKind.File).ToList();
      }

      #endregion

      #region [ Text ]

      /// <summary>
      /// Reads blob content and converts to text in UTF-8 encoding
      /// </summary>
      /// <param name="provider"></param>
      /// <param name="id">Blob id</param>
      /// <param name="cancellationToken"></param>
      /// <returns></returns>
      public static async Task<string> ReadTextAsync(
         this IBlobStorage provider,
         string id, CancellationToken cancellationToken = default)
      {
         Stream src = await provider.OpenReadAsync(id, cancellationToken);
         if (src == null) return null;

         var ms = new MemoryStream();
         using (src)
         {
            await src.CopyToAsync(ms);
         }

         return Encoding.UTF8.GetString(ms.ToArray());
      }

      /// <summary>
      /// Converts text to blob content and writes to storage
      /// </summary>
      /// <param name="provider"></param>
      /// <param name="id">Blob id</param>
      /// <param name="text">Text to write, treated in UTF-8 encoding</param>
      /// <param name="cancellationToken"></param>
      /// <returns></returns>
      public static async Task WriteTextAsync(
         this IBlobStorage provider,
         string id, string text, CancellationToken cancellationToken = default)
      {
         using (Stream s = text.ToMemoryStream())
         {
            await provider.WriteAsync(id, s, false, cancellationToken);
         }
      }

      #endregion

      #region [ Singletons ]

      /// <summary>
      /// Checksi if blobs exists in the storage
      /// </summary>
      public static async Task<bool> ExistsAsync(this IBlobStorage blobStorage,
         string id, CancellationToken cancellationToken = default)
      {
         IEnumerable<bool> r = await blobStorage.ExistsAsync(new[] { id }, cancellationToken);
         return r.First();
      }

      /// <summary>
      /// Deletes a single blob
      /// </summary>
      /// <param name="storage"></param>
      /// <param name="id"></param>
      /// <param name="cancellationToken"></param>
      /// <returns></returns>
      public static Task DeleteAsync(
         this IBlobStorage storage,
         string id, CancellationToken cancellationToken = default)
      {
         return storage.DeleteAsync(new[] {id}, cancellationToken);
      }

      /// <summary>
      /// Gets basic blob metadata
      /// </summary>
      /// <returns>Blob metadata or null if blob doesn't exist</returns>
      public static async Task<BlobMeta> GetMetaAsync(this IBlobStorage storage,
         string id, CancellationToken cancellationToken = default)
      {
         return (await storage.GetMetaAsync(new[] { id }, cancellationToken)).First();
      }

      #endregion

      #region [ Streaming ]

      /// <summary>
      /// Downloads blob to a stream
      /// </summary>
      /// <param name="provider"></param>
      /// <param name="id">Blob ID, required</param>
      /// <param name="targetStream">Target stream to copy to, required</param>
      /// <param name="cancellationToken"></param>
      /// <exception cref="System.ArgumentNullException">Thrown when any parameter is null</exception>
      /// <exception cref="System.ArgumentException">Thrown when ID is too long. Long IDs are the ones longer than 50 characters.</exception>
      /// <exception cref="StorageException">Thrown when blob does not exist, error code set to <see cref="ErrorCode.NotFound"/></exception>
      public static async Task ReadToStreamAsync(
         this IBlobStorage provider,
         string id, Stream targetStream, CancellationToken cancellationToken = default)
      {
         if (targetStream == null)
            throw new ArgumentNullException(nameof(targetStream));

         Stream src = await provider.OpenReadAsync(id, cancellationToken);
         if (src == null) return;

         using (src)
         {
            await src.CopyToAsync(targetStream, BufferSize, cancellationToken);
         }
      }

      #endregion

      #region [ Files ]

      /// <summary>
      /// Downloads a blob to the local filesystem.
      /// </summary>
      /// <param name="provider"></param>
      /// <param name="id">Blob ID to download</param>
      /// <param name="filePath">Full path to the local file to be downloaded to. If the file exists it will be recreated wtih blob data.</param>
      /// <param name="cancellationToken"></param>
      public static async Task ReadToFileAsync(
         this IBlobStorage provider,
         string id, string filePath, CancellationToken cancellationToken = default)
      {
         Stream src = await provider.OpenReadAsync(id, cancellationToken);
         if (src == null) return;

         using (src)
         {
            using (Stream dest = File.Create(filePath))
            {
               await src.CopyToAsync(dest, BufferSize, cancellationToken);
               await dest.FlushAsync();
            }
         }
      }

      /// <summary>
      /// Uploads local file to the blob storage
      /// </summary>
      /// <param name="provider"></param>
      /// <param name="id">Blob ID to create or overwrite</param>
      /// <param name="filePath">Path to local file</param>
      /// <param name="cancellationToken"></param>
      public static async Task WriteFileAsync(
         this IBlobStorage provider,
         string id, string filePath, CancellationToken cancellationToken = default(CancellationToken))
      {
         using (Stream src = File.OpenRead(filePath))
         {
            await provider.WriteAsync(id, src, false, cancellationToken);
         }
      }

      #endregion

      #region [ Uniqueue ]

      /// <summary>
      /// Copies blob to another storage
      /// </summary>
      /// <param name="provider"></param>
      /// <param name="blobId">Blob ID to copy</param>
      /// <param name="targetStorage">Target storage</param>
      /// <param name="newId">Optional, when specified uses this id in the target storage. If null uses the original ID.</param>
      /// <param name="cancellationToken"></param>
      public static async Task CopyToAsync(
         this IBlobStorage provider,
         string blobId, IBlobStorage targetStorage, string newId, CancellationToken cancellationToken = default)
      {
         Stream src = await provider.OpenReadAsync(blobId, cancellationToken);
         if (src == null) return;

         using (src)
         {
            await targetStorage.WriteAsync(newId ?? blobId, src, false, cancellationToken);
         }
      }

      #endregion
   }
}