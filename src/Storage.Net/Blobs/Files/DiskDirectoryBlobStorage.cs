using NetBox.Model;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using NetBox.Extensions;
using NetBox;

namespace Storage.Net.Blobs.Files
{
   /// <summary>
   /// Blob storage implementation which uses local file system directory
   /// </summary>
   internal class DiskDirectoryBlobStorage : IBlobStorage
   {
      private readonly string _directoryFullName;
      private const string AttributesFileExtension = ".attr";

      /// <summary>
      /// Creates an instance in a specific disk directory
      /// <param name="directoryFullName">Root directory</param>
      /// </summary>
      public DiskDirectoryBlobStorage(string directoryFullName)
      {
         if(directoryFullName == null)
            throw new ArgumentNullException(nameof(directoryFullName));

         _directoryFullName = Path.GetFullPath(directoryFullName);
      }

      /// <summary>
      /// Returns the list of blob names in this storage, optionally filtered by prefix
      /// </summary>
      public Task<IReadOnlyCollection<Blob>> ListAsync(ListOptions options, CancellationToken cancellationToken)
      {
         if(options == null) options = new ListOptions();

         GenericValidation.CheckBlobPrefix(options.FilePrefix);

         if(!Directory.Exists(_directoryFullName)) return Task.FromResult<IReadOnlyCollection<Blob>>(new List<Blob>());

         string fullPath = GetFolder(options?.FolderPath, false);
         if(fullPath == null) return Task.FromResult<IReadOnlyCollection<Blob>>(new List<Blob>());

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

         var result = new List<Blob>();
         result.AddRange(directoryIds.Select(id => ToBlobItem(id, BlobItemKind.Folder, options.IncludeAttributes)));
         result.AddRange(
            fileIds.Where(fid => !fid.EndsWith(AttributesFileExtension)).Select(id => ToBlobItem(id, BlobItemKind.File, options.IncludeAttributes)));
         result = result
            .Where(i => options.BrowseFilter == null || options.BrowseFilter(i))
            .Take(options.MaxResults == null ? int.MaxValue : options.MaxResults.Value)
            .ToList();
         return Task.FromResult<IReadOnlyCollection<Blob>>(result);
      }

      private static string FormatFlags(FileAttributes fa)
      {
         return string.Join("",
            fa.ToString().Split(',').Select(v => v.Trim().Substring(0, 1).ToUpper()).OrderBy(l => l));
      }

      private Blob ToBlobItem(string fullPath, BlobItemKind kind, bool includeMeta)
      {

         string relPath = fullPath.Substring(_directoryFullName.Length);
         relPath = relPath.Replace(Path.DirectorySeparatorChar, StoragePath.PathSeparator);
         relPath = relPath.Trim(StoragePath.PathSeparator);
         relPath = StoragePath.PathSeparatorString + relPath;

         if(kind == BlobItemKind.File)
         {
            var fi = new FileInfo(fullPath);

            var blob = new Blob(relPath, kind);
            blob.Size = fi.Length;
            blob.LastModificationTime = fi.CreationTimeUtc;
            blob.TryAddProperties(
               "IsReadOnly", fi.IsReadOnly.ToString(),
               "LastAccessTimeUtc", fi.LastAccessTimeUtc.ToString(),
               "LastWriteTimeUtc", fi.LastWriteTimeUtc.ToString(),
               "Attributes", FormatFlags(fi.Attributes));

            if(includeMeta)
            {
               EnrichWithMetadata(blob);
            }

            return blob;
         }
         else
         {
            var di = new DirectoryInfo(fullPath);

            var blob = new Blob(relPath, BlobItemKind.Folder);
            blob.LastModificationTime = di.CreationTimeUtc;
            blob.TryAddProperties(
               "LastAccessTimeUtc", di.LastAccessTimeUtc.ToString(),
               "LastWriteTimeUtc", di.LastWriteTimeUtc.ToString(),
               "Attributes", FormatFlags(di.Attributes));

            if(includeMeta)
            {
               EnrichWithMetadata(blob);
            }

            return blob;
         }
      }

      private string GetFolder(string path, bool createIfNotExists)
      {
         if(path == null) return _directoryFullName;
         string[] parts = StoragePath.Split(path);

         string fullPath = _directoryFullName;

         foreach(string part in parts)
         {
            fullPath = Path.Combine(fullPath, part);
         }

         if(!Directory.Exists(fullPath))
         {
            if(createIfNotExists)
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

      private string GetFilePath(string fullPath, bool createIfNotExists = true)
      {
         //id can contain path separators
         fullPath = fullPath.Trim(StoragePath.PathSeparator);
         string[] parts = fullPath.Split(StoragePath.PathSeparator).Select(EncodePathPart).ToArray();
         string name = parts[parts.Length - 1];
         string dir;
         if(parts.Length == 1)
         {
            dir = _directoryFullName;
         }
         else
         {
            string extraPath = string.Join(StoragePath.PathSeparatorString, parts, 0, parts.Length - 1);

            fullPath = Path.Combine(_directoryFullName, extraPath);

            dir = fullPath;
            if(!Directory.Exists(dir))
               Directory.CreateDirectory(dir);
         }

         return Path.Combine(dir, name);
      }

      private Stream CreateStream(string fullPath, bool overwrite = true)
      {
         GenericValidation.CheckBlobFullPath(fullPath);
         if(!Directory.Exists(_directoryFullName)) Directory.CreateDirectory(_directoryFullName);
         string path = GetFilePath(fullPath);

         Directory.CreateDirectory(Path.GetDirectoryName(path));
         Stream s = overwrite ? File.Create(path) : File.OpenWrite(path);
         s.Seek(0, SeekOrigin.End);
         return s;
      }

      private Stream OpenStream(string fullPath)
      {
         GenericValidation.CheckBlobFullPath(fullPath);
         string path = GetFilePath(fullPath);
         if(!File.Exists(path)) return null;

         return File.OpenRead(path);
      }

      private static string EncodePathPart(string path)
      {
         return path;
         //return path.UrlEncode();
      }

      private static string DecodePathPart(string path)
      {
         return path;
         //return path.UrlDecode();
      }

      /// <summary>
      /// dispose
      /// </summary>
      public void Dispose()
      {
      }

      public Task WriteAsync(string fullPath, Stream dataStream, bool append, CancellationToken cancellationToken)
      {
         if(dataStream is null)
            throw new ArgumentNullException(nameof(dataStream));
         GenericValidation.CheckBlobFullPath(fullPath);

         fullPath = StoragePath.Normalize(fullPath, false);

         using(Stream stream = CreateStream(fullPath, !append))
         {
            dataStream.CopyTo(stream);
         }

         return Task.FromResult(true);
      }

      /// <summary>
      /// Opens file and returns the open stream
      /// </summary>
      public Task<Stream> OpenReadAsync(string fullPath, CancellationToken cancellationToken)
      {
         GenericValidation.CheckBlobFullPath(fullPath);

         fullPath = StoragePath.Normalize(fullPath, false);
         Stream result = OpenStream(fullPath);

         return Task.FromResult(result);
      }

      /// <summary>
      /// Deletes files if they exist
      /// </summary>
      public Task DeleteAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken)
      {
         if(fullPaths == null) return Task.FromResult(true);

         foreach(string fullPath in fullPaths)
         {
            GenericValidation.CheckBlobFullPath(fullPath);

            string path = GetFilePath(StoragePath.Normalize(fullPath, false));
            if(File.Exists(path))
            {
               File.Delete(path);
            }
            else if(Directory.Exists(path))
            {
               Directory.Delete(path, true);
            }
         }

         return Task.FromResult(true);
      }

      /// <summary>
      /// Checks if files exist on disk
      /// </summary>
      public Task<IReadOnlyCollection<bool>> ExistsAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken)
      {
         var result = new List<bool>();

         if(fullPaths != null)
         {
            GenericValidation.CheckBlobFullPaths(fullPaths);

            foreach(string fullPath in fullPaths)
            {
               bool exists = File.Exists(GetFilePath(StoragePath.Normalize(fullPath, false)));
               result.Add(exists);
            }
         }

         return Task.FromResult((IReadOnlyCollection<bool>)result);
      }

      /// <summary>
      /// See interface
      /// </summary>
      public Task<IReadOnlyCollection<Blob>> GetBlobsAsync(IEnumerable<string> ids, CancellationToken cancellationToken = default)
      {
         var result = new List<Blob>();

         foreach(string blobId in ids)
         {
            GenericValidation.CheckBlobFullPath(blobId);

            string filePath = GetFilePath(blobId, false);

            if(!File.Exists(filePath))
            {
               result.Add(null);
               continue;
            }

            result.Add(ToBlobItem(filePath, BlobItemKind.File, true));
         }

         return Task.FromResult<IReadOnlyCollection<Blob>>(result);
      }

      public Task SetBlobsAsync(IEnumerable<Blob> blobs, CancellationToken cancellationToken = default)
      {
         GenericValidation.CheckBlobFullPaths(blobs);

         foreach(Blob blob in blobs.Where(b => b != null))
         {
            string blobPath = GetFilePath(blob.FullPath);

            if(!File.Exists(blobPath))
               continue;

            if(blob?.Metadata == null)
               continue;

            string attrPath = GetFilePath(blob.FullPath) + AttributesFileExtension;
            File.WriteAllBytes(attrPath, blob.AttributesToByteArray());
         }

         return Task.CompletedTask;
      }

      private void EnrichWithMetadata(Blob blob)
      {
         string path = GetFilePath(StoragePath.Normalize(blob.FullPath, false));

         if (!File.Exists(path)) return;

         var fi = new FileInfo(path);

         try
         {
            string attrFilePath = path + AttributesFileExtension;
            if(File.Exists(attrFilePath))
            {
               byte[] content = File.ReadAllBytes(attrFilePath);
               blob.AppendAttributesFromByteArray(content);
            }
         }
         catch(IOException)
         {
            //sometimes files are locked, inaccessible etc.
         }
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
