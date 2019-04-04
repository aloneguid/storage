using NetBox.Model;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using NetBox.Extensions;
using NetBox;

namespace Storage.Net.Blob.Files
{
   /// <summary>
   /// Blob storage implementation which uses local file system directory
   /// </summary>
   public class DiskDirectoryBlobStorage : IBlobStorage
   {
      private readonly DirectoryInfo _directory;

      /// <summary>
      /// Creates an instance in a specific disk directory
      /// <param name="directory">Root directory</param>
      /// </summary>
      public DiskDirectoryBlobStorage(DirectoryInfo directory)
      {
         _directory = directory;
      }

      /// <summary>
      /// Original root directory this storage is mapped to
      /// </summary>
      public DirectoryInfo RootDirectory => _directory;

      /// <summary>
      /// Returns the list of blob names in this storage, optionally filtered by prefix
      /// </summary>
      public Task<IReadOnlyCollection<BlobId>> ListAsync(ListOptions options, CancellationToken cancellationToken)
      {
         if (options == null) options = new ListOptions();

         GenericValidation.CheckBlobPrefix(options.FilePrefix);

         if (!_directory.Exists) return Task.FromResult<IReadOnlyCollection<BlobId>>(new List<BlobId>());

         string fullPath = GetFolder(options?.FolderPath, false);
         if (fullPath == null) return Task.FromResult<IReadOnlyCollection<BlobId>>(new List<BlobId>());

         string[] fileIds = Directory.GetFiles(
            fullPath,
            string.IsNullOrEmpty(options.FilePrefix)
               ? "*"
               : options.FilePrefix + "*",
            options.Recurse ? SearchOption.AllDirectories : SearchOption.TopDirectoryOnly);

         string[] directoryIds = Directory.GetDirectories(
               fullPath,
               string.IsNullOrEmpty(options.FilePrefix)
                  ? "*"
                  : options.FilePrefix + "*",
               options.Recurse ? SearchOption.AllDirectories : SearchOption.TopDirectoryOnly);

         var result = new List<BlobId>();
         result.AddRange(directoryIds.Select(id => ToBlobItem(id, BlobItemKind.Folder, options.IncludeMetaWhenKnown)));
         result.AddRange(fileIds.Select(id => ToBlobItem(id, BlobItemKind.File, options.IncludeMetaWhenKnown)));
         result = result
            .Where(i => options.BrowseFilter == null || options.BrowseFilter(i))
            .Take(options.MaxResults == null ? int.MaxValue : options.MaxResults.Value)
            .ToList();
         return Task.FromResult<IReadOnlyCollection<BlobId>>(result);
      }

      private string ToId(FileInfo fi)
      {
         string name = fi.FullName.Substring(_directory.FullName.Length + 1);

         name = name.Replace(Path.DirectorySeparatorChar, StoragePath.PathSeparator);

         string[] parts = name.Split(StoragePath.PathSeparator);

         return string.Join(StoragePath.PathStrSeparator, parts.Select(DecodePathPart));
      }

      private BlobId ToBlobItem(string fullPath, BlobItemKind kind, bool includeMeta)
      {
         string id = Path.GetFileName(fullPath);

         fullPath = fullPath.Substring(_directory.FullName.Length);
         fullPath = fullPath.Replace(Path.DirectorySeparatorChar, StoragePath.PathSeparator);
         fullPath = fullPath.Trim(StoragePath.PathSeparator);
         fullPath = StoragePath.PathStrSeparator + fullPath;

         var blobId = new BlobId(fullPath, kind);

         if(includeMeta)
         {
            blobId.Meta = BlobMetaFromId(blobId.FullPath);
         }

         return blobId;
      }

      private string GetFolder(string path, bool createIfNotExists)
      {
         if (path == null) return _directory.FullName;
         string[] parts = StoragePath.Split(path);

         string fullPath = _directory.FullName;

         foreach (string part in parts)
         {
            fullPath = Path.Combine(fullPath, part);
         }

         if (!Directory.Exists(fullPath))
         {
            if (createIfNotExists)
            {
               Directory.CreateDirectory(fullPath);
            }
            else
            {
               return null;
            }
         }

         return fullPath;
      }

      private string GetFilePath(string id, bool createIfNotExists = true)
      {
         //id can contain path separators
         id = id.Trim(StoragePath.PathSeparator);
         string[] parts = id.Split(StoragePath.PathSeparator).Select(EncodePathPart).ToArray();
         string name = parts[parts.Length - 1];
         DirectoryInfo dir;
         if(parts.Length == 1)
         {
            dir = _directory;
         }
         else
         {
            string extraPath = string.Join(StoragePath.PathStrSeparator, parts, 0, parts.Length - 1);

            string fullPath = Path.Combine(_directory.FullName, extraPath);

            dir = new DirectoryInfo(fullPath);
            if (!dir.Exists) dir.Create();
         }

         return Path.Combine(dir.FullName, name);
      }

      private Stream CreateStream(string id, bool overwrite = true)
      {
         GenericValidation.CheckBlobId(id);
         if (!_directory.Exists) _directory.Create();
         string path = GetFilePath(id);

         Stream s = overwrite ? File.Create(path) : File.OpenWrite(path);
         s.Seek(0, SeekOrigin.End);
         return s;
      }

      private Stream OpenStream(string id)
      {
         GenericValidation.CheckBlobId(id);
         string path = GetFilePath(id);
         if(!File.Exists(path)) return null;

         return File.OpenRead(path);
      }

      private static string EncodePathPart(string path)
      {
         return path.UrlEncode();
      }

      private static string DecodePathPart(string path)
      {
         return path.UrlDecode();
      }

      /// <summary>
      /// dispose
      /// </summary>
      public void Dispose()
      {
      }

      /// <summary>
      /// Streams into file
      /// </summary>
      public async Task WriteAsync(string id, Stream sourceStream, bool append, CancellationToken cancellationToken)
      {
         GenericValidation.CheckBlobId(id);
         GenericValidation.CheckSourceStream(sourceStream);

         id = StoragePath.Normalize(id, false);
         using (Stream dest = CreateStream(id, !append))
         {
           await sourceStream.CopyToAsync(dest);
         }
      }

      /// <summary>
      /// 
      /// </summary>
      public Task<Stream> OpenWriteAsync(string id, bool append, CancellationToken cancellationToken)
      {
         GenericValidation.CheckBlobId(id);

         id = StoragePath.Normalize(id, false);

         return Task.FromResult(CreateStream(id, !append));
      }

      /// <summary>
      /// Opens file and returns the open stream
      /// </summary>
      public Task<Stream> OpenReadAsync(string id, CancellationToken cancellationToken)
      {
         GenericValidation.CheckBlobId(id);

         id = StoragePath.Normalize(id, false);
         Stream result = OpenStream(id);

         return Task.FromResult(result);
      }

      /// <summary>
      /// Deletes files if they exist
      /// </summary>
      public Task DeleteAsync(IEnumerable<string> ids, CancellationToken cancellationToken)
      {
         if (ids == null) return Task.FromResult(true);

         foreach (string id in ids)
         {
            GenericValidation.CheckBlobId(id);

            string path = GetFilePath(StoragePath.Normalize(id, false));
            if (File.Exists(path)) File.Delete(path);
         }

         return Task.FromResult(true);
      }

      /// <summary>
      /// Checks if files exist on disk
      /// </summary>
      public Task<IReadOnlyCollection<bool>> ExistsAsync(IEnumerable<string> ids, CancellationToken cancellationToken)
      {
         var result = new List<bool>();

         if (ids != null)
         {
            GenericValidation.CheckBlobId(ids);

            foreach (string id in ids)
            {
               bool exists = File.Exists(GetFilePath(StoragePath.Normalize(id, false)));
               result.Add(exists);
            }
         }

         return Task.FromResult((IReadOnlyCollection<bool>)result);
      }

      /// <summary>
      /// Gets file metadata
      /// </summary>
      public Task<IEnumerable<BlobMeta>> GetMetaAsync(IEnumerable<string> ids, CancellationToken cancellationToken)
      {
         if (ids == null) return Task.FromResult<IEnumerable<BlobMeta>>(null);

         GenericValidation.CheckBlobId(ids);

         var result = new List<BlobMeta>();

         foreach (string id in ids)
         {
            result.Add(BlobMetaFromId(id));
         }

         return Task.FromResult((IEnumerable<BlobMeta>) result);
      }

      private BlobMeta BlobMetaFromId(string id)
      {
         string path = GetFilePath(StoragePath.Normalize(id, false));

         if (!File.Exists(path)) return null;

         var fi = new FileInfo(path);

         string md5;
         using (Stream fs = File.OpenRead(fi.FullName))
         {
            md5 = fs.GetHash(HashType.Md5);
         }

         var meta = new BlobMeta(
            fi.Length,
            md5,
            fi.CreationTimeUtc);

         return meta;
      }

      /// <summary>
      /// Returns empty transaction as filesystem has no transaction support
      /// </summary>
      public Task<ITransaction> OpenTransactionAsync()
      {
         return Task.FromResult(EmptyTransaction.Instance);
      }
   }
}
