using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using NetBox;
using NetBox.Extensions;

namespace Storage.Net.Blobs
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
      /// <param name="blobStorage"></param>
      /// <param name="options"></param>
      /// <param name="cancellationToken"></param>
      /// <returns>List of blob IDs</returns>
      public static async Task<IReadOnlyCollection<Blob>> ListFilesAsync(this IBlobStorage blobStorage,
         ListOptions options,
         CancellationToken cancellationToken = default)
      {
         IReadOnlyCollection<Blob> all = await blobStorage.ListAsync(options, cancellationToken).ConfigureAwait(false);

         return all.Where(i => i!= null && i.IsFile).ToList();
      }

      /// <summary>
      /// Returns the list of available blobs
      /// </summary>
      /// <param name="blobStorage"></param>
      /// <param name="folderPath"><see cref="ListOptions.FolderPath"/></param>
      /// <param name="browseFilter"><see cref="ListOptions.BrowseFilter"/></param>
      /// <param name="filePrefix"><see cref="ListOptions.FilePrefix"/></param>
      /// <param name="recurse"><see cref="ListOptions.Recurse"/></param>
      /// <param name="maxResults"><see cref="ListOptions.MaxResults"/></param>
      /// <param name="includeAttributes"><see cref="ListOptions.IncludeAttributes"/></param>
      /// <param name="cancellationToken"></param>
      /// <returns>List of blob IDs</returns>
      public static Task<IReadOnlyCollection<Blob>> ListAsync(this IBlobStorage blobStorage,
         string folderPath = null,
         Func<Blob, bool> browseFilter = null,
         string filePrefix = null,
         bool recurse = false,
         int? maxResults = null,
         bool includeAttributes = false,
         CancellationToken cancellationToken = default)
      {
         var options = new ListOptions();
         if(folderPath != null)
            options.FolderPath = folderPath;
         if(browseFilter != null)
            options.BrowseFilter = browseFilter;
         if(filePrefix != null)
            options.FilePrefix = filePrefix;
         options.Recurse = recurse;
         if(maxResults != null)
            options.MaxResults = maxResults;
         options.IncludeAttributes = includeAttributes;

         return blobStorage.ListAsync(options, cancellationToken);
      }

      #endregion

      #region [ Text ]

      /// <summary>
      /// Reads blob content and converts to text in UTF-8 encoding
      /// </summary>
      /// <param name="provider"></param>
      /// <param name="fullPath">Blob id</param>
      /// <param name="textEncoding">Optional text encoding. When not specified, <see cref="UTF8Encoding"/> is used.</param>
      /// <param name="cancellationToken"></param>
      /// <returns></returns>
      public static async Task<string> ReadTextAsync(
         this IBlobStorage provider,
         string fullPath,
         Encoding textEncoding = null,
         CancellationToken cancellationToken = default)
      {
         Stream src = await provider.OpenReadAsync(fullPath, cancellationToken).ConfigureAwait(false);
         if (src == null) return null;

         var ms = new MemoryStream();
         using (src)
         {
            await src.CopyToAsync(ms).ConfigureAwait(false);
         }

         return (textEncoding ?? Encoding.UTF8).GetString(ms.ToArray());
      }

      /// <summary>
      /// Converts text to blob content and writes to storage
      /// </summary>
      /// <param name="provider"></param>
      /// <param name="fullPath">Blob to write</param>
      /// <param name="text">Text to write, treated in UTF-8 encoding</param>
      /// <param name="textEncoding">Optional text encoding. When not specified, <see cref="UTF8Encoding"/> is used.</param>
      /// <param name="cancellationToken"></param>
      /// <returns></returns>
      public static async Task WriteTextAsync(
         this IBlobStorage provider,
         string fullPath, string text,
         Encoding textEncoding = null,
         CancellationToken cancellationToken = default)
      {
         using (Stream s = text.ToMemoryStream(textEncoding ?? Encoding.UTF8))
         {
            await provider.WriteAsync(fullPath, s, false, cancellationToken).ConfigureAwait(false);
         }
      }

      #endregion

      #region [ Singletons ]

      /// <summary>
      /// Checksi if blobs exists in the storage
      /// </summary>
      public static async Task<bool> ExistsAsync(this IBlobStorage blobStorage,
         string fullPath, CancellationToken cancellationToken = default)
      {
         IEnumerable<bool> r = await blobStorage.ExistsAsync(new[] { fullPath }, cancellationToken).ConfigureAwait(false);
         return r.First();
      }

      /// <summary>
      /// Deletes a single blob or a folder recursively.
      /// </summary>
      /// <param name="storage"></param>
      /// <param name="fullPath"></param>
      /// <param name="cancellationToken"></param>
      /// <returns></returns>
      public static Task DeleteAsync(
         this IBlobStorage storage,
         string fullPath, CancellationToken cancellationToken = default)
      {
         return storage.DeleteAsync(new[] {fullPath}, cancellationToken);
      }

      /// <summary>
      /// Deletes a collection of blobs or folders
      /// </summary>
      public static Task DeleteAsync(
         this IBlobStorage storage,
         IEnumerable<Blob> blobs,
         CancellationToken cancellationToken = default)
      {
         return storage.DeleteAsync(blobs.Select(b => b.FullPath), cancellationToken);
      }

      /// <summary>
      /// Gets basic blob metadata
      /// </summary>
      /// <returns>Blob metadata or null if blob doesn't exist</returns>
      public static async Task<Blob> GetBlobAsync(this IBlobStorage storage,
         string fullPath, CancellationToken cancellationToken = default)
      {
         return (await storage.GetBlobsAsync(new[] { fullPath }, cancellationToken).ConfigureAwait(false)).First();
      }

      /// <summary>
      /// Set blob attributes
      /// </summary>
      public static Task SetBlobAsync(this IBlobStorage storage,
         Blob blob, CancellationToken cancellationToken = default)
      {
         return storage.SetBlobsAsync(new[] { blob }, cancellationToken);
      }

      #endregion

      #region [ Bytes ]

      /// <summary>
      /// Writes byte array to the target storage. If you can, never use large byte arrays, they are terrible!
      /// </summary>
      public static async Task WriteAsync(this IBlobStorage provider, string fullPath, byte[] data, bool append = false, CancellationToken cancellationToken = default)
      {
         if (data == null)
         {
            throw new ArgumentNullException(nameof(data));
         }

         using (var source = new MemoryStream(data))
         {
            await provider.WriteAsync(fullPath, source, append, cancellationToken).ConfigureAwait(false);
         }
      }

      /// <summary>
      /// Reads blob content as byte array
      /// </summary>
      public static async Task<byte[]> ReadBytesAsync(this IBlobStorage storage, string fullPath, CancellationToken cancellationToken = default)
      {
         Stream src = await storage.OpenReadAsync(fullPath, cancellationToken).ConfigureAwait(false);
         if (src == null) return null;

         var ms = new MemoryStream();
         using (src)
         {
            await src.CopyToAsync(ms).ConfigureAwait(false);
         }

         return ms.ToArray();
      }

      #endregion

      #region [ Streaming ]

      /// <summary>
      /// Downloads blob to a stream
      /// </summary>
      /// <param name="provider"></param>
      /// <param name="fullPath">Blob ID, required</param>
      /// <param name="targetStream">Target stream to copy to, required</param>
      /// <param name="cancellationToken"></param>
      /// <exception cref="System.ArgumentNullException">Thrown when any parameter is null</exception>
      /// <exception cref="System.ArgumentException">Thrown when ID is too long. Long IDs are the ones longer than 50 characters.</exception>
      /// <exception cref="StorageException">Thrown when blob does not exist, error code set to <see cref="ErrorCode.NotFound"/></exception>
      public static async Task ReadToStreamAsync(
         this IBlobStorage provider,
         string fullPath, Stream targetStream, CancellationToken cancellationToken = default)
      {
         if (targetStream == null)
            throw new ArgumentNullException(nameof(targetStream));

         Stream src = await provider.OpenReadAsync(fullPath, cancellationToken).ConfigureAwait(false);
         if (src == null) return;

         using (src)
         {
            await src.CopyToAsync(targetStream, BufferSize, cancellationToken).ConfigureAwait(false);
         }
      }

      #endregion

      #region [ Files ]

      /// <summary>
      /// Downloads a blob to the local filesystem.
      /// </summary>
      /// <param name="provider"></param>
      /// <param name="fullPath">Blob ID to download</param>
      /// <param name="filePath">Full path to the local file to be downloaded to. If the file exists it will be recreated wtih blob data.</param>
      /// <param name="cancellationToken"></param>
      public static async Task ReadToFileAsync(
         this IBlobStorage provider,
         string fullPath, string filePath, CancellationToken cancellationToken = default)
      {
         Stream src = await provider.OpenReadAsync(fullPath, cancellationToken).ConfigureAwait(false);
         if (src == null) return;

         using (src)
         {
            using (Stream dest = File.Create(filePath))
            {
               await src.CopyToAsync(dest, BufferSize, cancellationToken).ConfigureAwait(false);
               await dest.FlushAsync().ConfigureAwait(false);
            }
         }
      }

      /// <summary>
      /// Uploads local file to the blob storage
      /// </summary>
      /// <param name="provider"></param>
      /// <param name="fullPath">Blob ID to create or overwrite</param>
      /// <param name="filePath">Path to local file</param>
      /// <param name="cancellationToken"></param>
      public static async Task WriteFileAsync(
         this IBlobStorage provider,
         string fullPath, string filePath, CancellationToken cancellationToken = default)
      {
         using (Stream src = File.OpenRead(filePath))
         {
            await provider.WriteAsync(fullPath, src, false, cancellationToken).ConfigureAwait(false);
         }
      }

      #endregion

#region [ Objects ]

      /// <summary>
      /// Writes an object to blob storage using <see cref="JsonSerializer"/>
      /// </summary>
      /// <typeparam name="T">Objec type</typeparam>
      /// <param name="storage"></param>
      /// <param name="fullPath">Full path to blob</param>
      /// <param name="instance">Object instance to write</param>
      /// <param name="options">Optional serialiser options</param>
      /// <param name="encoding">Text encoding used to write to the blob storage, defaults to <see cref="UTF8Encoding"/></param>
      /// <param name="cancellationToken"></param>
      /// <returns></returns>
      public static async Task WriteJsonAsync<T>(
         this IBlobStorage storage,
         string fullPath, T instance,
         JsonSerializerOptions options = null,
         Encoding encoding = null,
         CancellationToken cancellationToken = default)
      {
         string jsonText = JsonSerializer.Serialize(instance, options);
         await WriteTextAsync(storage, fullPath, jsonText, encoding, cancellationToken).ConfigureAwait(false);
      }

      /// <summary>
      /// Reads an object from blob storage using <see cref="JsonSerializer"/>
      /// </summary>
      /// <typeparam name="T"></typeparam>
      /// <param name="storage"></param>
      /// <param name="fullPath">Full path to blob</param>
      /// <param name="ignoreInvalidJson">When true, json that cannot be deserialised is ignored and method simply returns default value</param>
      /// <param name="options">Optional serialiser options</param>
      /// <param name="encoding">Text encoding used to write to the blob storage, defaults to <see cref="UTF8Encoding"/></param>
      /// <param name="cancellationToken"></param>
      /// <returns></returns>
      public static async Task<T> ReadJsonAsync<T>(this IBlobStorage storage,
         string fullPath,
         bool ignoreInvalidJson = false,
         JsonSerializerOptions options = null,
         Encoding encoding = null,
         CancellationToken cancellationToken = default)
      {
         string jsonText = await storage.ReadTextAsync(fullPath, encoding, cancellationToken).ConfigureAwait(false);
         if(string.IsNullOrEmpty(jsonText))
            return default;

         try
         {
            return JsonSerializer.Deserialize<T>(jsonText, options);
         }
         catch(JsonException)
         {
            if(ignoreInvalidJson)
               return default;

            throw;
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
         using(Stream src = await provider.OpenReadAsync(blobId, cancellationToken).ConfigureAwait(false))
         {
            if(src == null)
               return;

            await targetStorage.WriteAsync(newId ?? blobId, src, false, cancellationToken).ConfigureAwait(false);
         }
      }

      /// <summary>
      /// Calculates an MD5 hash of a blob. Comparing to <see cref="Blob.MD5"/> field, it always returns
      /// a hash, even if the underlying storage doesn't support it natively.
      /// </summary>
      public static async Task<string> GetMD5HashAsync(this IBlobStorage blobStorage, Blob blob, CancellationToken cancellationToken = default)
      {
         if(blob == null)
            throw new ArgumentNullException(nameof(blob));

         if(blob.MD5 != null)
            return blob.MD5;

         blob = await blobStorage.GetBlobAsync(blob.FullPath, cancellationToken).ConfigureAwait(false);

         if(blob.MD5 != null)
            return blob.MD5;

         //hash definitely not supported, calculate it manually

         using(Stream s = await blobStorage.OpenReadAsync(blob.FullPath, cancellationToken).ConfigureAwait(false))
         {
            if(s == null)
               return null;

            string hash = s.GetHash(HashType.Md5);

            return hash;
         }
      }

      /// <summary>
      /// Rename a blob (folder, file etc.).
      /// </summary>
      /// <param name="blobStorage"></param>
      /// <param name="oldPath"></param>
      /// <param name="newPath"></param>
      /// <param name="cancellationToken"></param>
      /// <returns></returns>
      public static async Task RenameAsync(this IBlobStorage blobStorage,
         string oldPath, string newPath, CancellationToken cancellationToken = default)
      {
         if(oldPath is null)
            throw new ArgumentNullException(nameof(oldPath));
         if(newPath is null)
            throw new ArgumentNullException(nameof(newPath));

         //try to use extended client here
         if(blobStorage is IExtendedBlobStorage extendedBlobStorage)
         {
            await extendedBlobStorage.RenameAsync(oldPath, newPath, cancellationToken).ConfigureAwait(false);
         }
         else
         {
            //this needs to be done recursively
            foreach(Blob item in await blobStorage.ListAsync(oldPath, recurse: true).ConfigureAwait(false))
            {
               if(item.IsFile)
               {
                  string renamedPath = item.FullPath.Replace(oldPath, newPath);

                  await blobStorage.CopyToAsync(item, blobStorage, renamedPath, cancellationToken).ConfigureAwait(false);
                  await blobStorage.DeleteAsync(item, cancellationToken).ConfigureAwait(false);
               }
            }

            //rename self
            await blobStorage.CopyToAsync(oldPath, blobStorage, newPath, cancellationToken).ConfigureAwait(false);
            await blobStorage.DeleteAsync(oldPath, cancellationToken).ConfigureAwait(false);
         }


      }

#endregion

#region [ Folders ]

      /// <summary>
      /// Creates a new folder in this storage. If storage supports hierarchy, the folder is created as is, otherwise a folder is created by putting a dummy zero size file in that folder.
      /// </summary>
      /// <param name="blobStorage"></param>
      /// <param name="folderPath">Path to the folder</param>
      /// <param name="dummyFileName">If storage doesn't support hierary, you can override the dummy file name created in that empty folder.</param>
      /// <param name="cancellationToken"></param>
      /// <returns></returns>
      public static async Task CreateFolderAsync(
         this IBlobStorage blobStorage, string folderPath, string dummyFileName = null, CancellationToken cancellationToken = default)
      {
         if(blobStorage is IHierarchicalBlobStorage hierarchicalBlobStorage)
         {
            await hierarchicalBlobStorage.CreateFolderAsync(folderPath, cancellationToken).ConfigureAwait(false);
         }
         else
         {
            string fullPath = StoragePath.Combine(folderPath, dummyFileName ?? ".empty");

            await blobStorage.WriteTextAsync(
               fullPath,
               "created as a workaround by Storage.Net when creating an empty parent folder",
               null,
               cancellationToken).ConfigureAwait(false);
         }
      }

#endregion
   }
}