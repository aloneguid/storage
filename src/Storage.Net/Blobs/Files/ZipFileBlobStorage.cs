using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Storage.Net.Streaming;

namespace Storage.Net.Blobs.Files
{
   class ZipFileBlobStorage : IBlobStorage
   {
      private Stream _fileStream;
      private ZipArchive _archive;
      private readonly string _filePath;
      private bool? _isWriteMode;

      public ZipFileBlobStorage(string filePath)
      {
         _filePath = filePath;
      }

      public void Dispose()
      {
         if(_archive != null)
         {
            _archive.Dispose();
            _archive = null;
         }

         if(_fileStream != null)
         {
            _fileStream.Flush();
            _fileStream.Dispose();
            _fileStream = null;
         }
      }

      public Task<IReadOnlyCollection<bool>> ExistsAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default)
      {
         ZipArchive zipArchive = GetArchive(false);
         if(zipArchive == null)
         {
            return Task.FromResult<IReadOnlyCollection<bool>>(new bool[fullPaths.Count()]);
         }

         var result = new List<bool>();

         foreach(string fullPath in fullPaths)
         {
            string nid = StoragePath.Normalize(fullPath);

            ZipArchiveEntry entry = zipArchive.GetEntry(nid);

            result.Add(entry != null);
         }

         return Task.FromResult<IReadOnlyCollection<bool>>(result);
      }

      public Task<IReadOnlyCollection<Blob>> GetBlobsAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default)
      {
         var result = new List<Blob>();
         ZipArchive zipArchive = GetArchive(false);

         foreach(string fullPath in fullPaths)
         {
            string nid = StoragePath.Normalize(fullPath);

            try
            {
               ZipArchiveEntry entry = zipArchive.GetEntry(nid);

               long originalLength = entry.Length;

               result.Add(new Blob(nid) { Size = originalLength, LastModificationTime = entry.LastWriteTime });
            }
            catch(NullReferenceException)
            {
               result.Add(null);
            }
         }

         return Task.FromResult<IReadOnlyCollection<Blob>>(result);
      }

      public Task SetBlobsAsync(IEnumerable<Blob> blobs, CancellationToken cancellationToken = default)
      {
         throw new NotSupportedException();
      }

      public Task<IReadOnlyCollection<Blob>> ListAsync(ListOptions options, CancellationToken cancellationToken = default)
      {
         if(!File.Exists(_filePath))
            return Task.FromResult<IReadOnlyCollection<Blob>>(new List<Blob>());

         ZipArchive archive = GetArchive(false);

         if(options == null)
            options = new ListOptions();

         IEnumerable<Blob> blobs = archive.Entries.Select(ze => new Blob(ze.FullName, BlobItemKind.File));

         if(options.FilePrefix != null)
            blobs = blobs.Where(id => id.Name.StartsWith(options.FilePrefix));

         //find ones that belong to this folder
         if(!StoragePath.IsRootPath(options.FolderPath))
         {
            blobs = blobs.Where(id => id.FullPath.StartsWith(options.FolderPath));
         }

         blobs = AppendVirtualFolders(options.FolderPath ?? StoragePath.RootFolderPath, blobs.ToList());

         //cut off sub-items
         if(!options.Recurse)
         {
            blobs = blobs
               .Select(b => new { rp = b.FullPath.Substring(options.FolderPath.Length + 1), b = b })
               .Where(a => !a.rp.Contains(StoragePath.PathSeparator))
               .Select(a => a.b);


            //blobs = blobs.Where(id => !id.FullPath.Substring(0, options.FolderPath.Length).Contains(StoragePath.PathSeparator));
         }

         if(options.BrowseFilter != null)
            blobs = blobs.Where(id => options.BrowseFilter(id));

         if(options.MaxResults != null)
            blobs = blobs.Take(options.MaxResults.Value);

         return Task.FromResult<IReadOnlyCollection<Blob>>(blobs.ToList());
      }

      private IEnumerable<Blob> AppendVirtualFolders(string rootFolderPath, List<Blob> files)
      {
         var uniqueFolders = new HashSet<string>(
            files.Where(f => f.FolderPath != rootFolderPath).Select(f => f.FolderPath));

         files.AddRange(uniqueFolders.Select(path => new Blob(path, BlobItemKind.Folder)));

         return files;
      }

      public Task<Stream> OpenReadAsync(string fullPath, CancellationToken cancellationToken = default)
      {
         fullPath = StoragePath.Normalize(fullPath);

         ZipArchive archive = GetArchive(false);
         if(archive == null)
            return Task.FromResult<Stream>(null);

         ZipArchiveEntry entry = archive.GetEntry(fullPath);
         if(entry == null)
            return Task.FromResult<Stream>(null);

         return Task.FromResult(entry.Open());
      }

      public Task<ITransaction> OpenTransactionAsync()
      {
         return Task.FromResult(EmptyTransaction.Instance);
      }

      public async Task WriteAsync(string fullPath, Stream dataStream, bool append, CancellationToken cancellationToken = default)
      {
         if(dataStream is null)
            throw new ArgumentNullException(nameof(dataStream));

         fullPath = StoragePath.Normalize(fullPath);

         using(var ms = new MemoryStream())
         {
            await dataStream.CopyToAsync(ms).ConfigureAwait(false);
            ms.Position = 0;

            ZipArchive archive = GetArchive(true);

            ZipArchiveEntry entry = archive.CreateEntry(fullPath, CompressionLevel.Optimal);
            using(Stream dest = entry.Open())
            {
               await ms.CopyToAsync(dest).ConfigureAwait(false);
               await dest.FlushAsync(cancellationToken).ConfigureAwait(false);
            }
         }
      }

      public Task DeleteAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default)
      {
         ZipArchive archive = GetArchive(true);

         foreach(string fullPath in fullPaths)
         {
            string nid = StoragePath.Normalize(fullPath);

            ZipArchiveEntry entry = archive.GetEntry(nid);
            if(entry != null)
            {
               entry.Delete();
            }
            else
            {
               //try to delete this as a folder
               string prefix = fullPath + StoragePath.PathSeparatorString;
               List<ZipArchiveEntry> folderEntries = archive.Entries.Where(e => e.FullName.StartsWith(prefix)).ToList();
               foreach(ZipArchiveEntry fi in folderEntries)
               {
                  fi.Delete();
               }
            }
         }

         return Task.CompletedTask;
      }

      private ZipArchive GetArchive(bool? forWriting)
      {
         if (_fileStream == null || _isWriteMode == null || _isWriteMode.Value != forWriting)
         {
            if (_fileStream != null)
            {
               if(forWriting == null)
               {
                  return _archive;
               }

               Dispose();
            }

            //check that directory exists, and create if not
            string dirPath = new FileInfo(_filePath).Directory.FullName;
            if(!Directory.Exists(dirPath))
               Directory.CreateDirectory(dirPath);

            bool exists = File.Exists(_filePath);

            if (forWriting != null && forWriting.Value)
            {
               _fileStream = File.Open(_filePath, FileMode.OpenOrCreate, FileAccess.ReadWrite);

               if (!exists)
               {
                  //create archive, then reopen in Update mode as certain operations only work in update mode

                  using (var archive = new ZipArchive(_fileStream, ZipArchiveMode.Create, true))
                  {

                  }

               }

               _archive = new ZipArchive(_fileStream,
                  ZipArchiveMode.Update,
                  true);
            }
            else
            {
               if (!exists) return null;

               _fileStream = File.Open(_filePath, FileMode.Open, FileAccess.Read);

               _archive = new ZipArchive(_fileStream, ZipArchiveMode.Read, true);
            }

            _isWriteMode = forWriting;

         }

         return _archive;
      }

      public Task MoveBlobAsync(string fromPath, string toPath, CancellationToken cancellationToken = default)
      {
         throw new NotImplementedException();
      }
   }
}
