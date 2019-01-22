using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Storage.Net.Blob;
using Storage.Net.Streaming;

namespace Storage.Net.ZipFile
{
   class ZipFileBlobStorageProvider : IBlobStorage
   {
      private Stream _fileStream;
      private ZipArchive _archive;
      private readonly string _filePath;
      private bool? _isWriteMode;

      public ZipFileBlobStorageProvider(string filePath)
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

      public Task<IReadOnlyCollection<bool>> ExistsAsync(IEnumerable<string> ids, CancellationToken cancellationToken = default(CancellationToken))
      {
         ZipArchive zipArchive = GetArchive(false);
         if(zipArchive == null)
         {
            return Task.FromResult<IReadOnlyCollection<bool>>(new bool[ids.Count()]);
         }

         var result = new List<bool>();

         foreach(string id in ids)
         {
            string nid = StoragePath.Normalize(id, false);

            ZipArchiveEntry entry = zipArchive.GetEntry(nid);

            result.Add(entry != null);
         }

         return Task.FromResult<IReadOnlyCollection<bool>>(result);
      }

      public Task<IEnumerable<BlobMeta>> GetMetaAsync(IEnumerable<string> ids, CancellationToken cancellationToken = default(CancellationToken))
      {
         var result = new List<BlobMeta>();
         ZipArchive zipArchive = GetArchive(false);

         foreach (string id in ids)
         {
            string nid = StoragePath.Normalize(id, false);

            try
            {
               ZipArchiveEntry entry = zipArchive.GetEntry(nid);

               long originalLength = entry.Length;

               result.Add(new BlobMeta(originalLength, null, entry.LastWriteTime));
            }
            catch (NullReferenceException)
            {
               result.Add(null);
            }
         }

         return Task.FromResult<IEnumerable<BlobMeta>>(result);
      }

      public Task<IReadOnlyCollection<BlobId>> ListAsync(ListOptions options, CancellationToken cancellationToken = default(CancellationToken))
      {
         if (!File.Exists(_filePath)) return Task.FromResult<IReadOnlyCollection<BlobId>>(new List<BlobId>());

         ZipArchive archive = GetArchive(false);

         if (options == null) options = new ListOptions();
         IEnumerable<BlobId> ids = archive.Entries.Select(ze => new BlobId(ze.FullName, BlobItemKind.File));
         if (options.FilePrefix != null) ids = ids.Where(id => id.Id.StartsWith(options.FilePrefix));
         if (options.BrowseFilter != null) ids = ids.Where(id => options.BrowseFilter(id));
         if (options.MaxResults != null) ids = ids.Take(options.MaxResults.Value);

         return Task.FromResult<IReadOnlyCollection<BlobId>>(ids.ToList());
      }

      public Task<Stream> OpenReadAsync(string id, CancellationToken cancellationToken = default(CancellationToken))
      {
         id = StoragePath.Normalize(id, false);

         ZipArchive archive = GetArchive(false);
         if (archive == null) return Task.FromResult<Stream>(null);

         ZipArchiveEntry entry = archive.GetEntry(id);
         if (entry == null) return Task.FromResult<Stream>(null);

         return Task.FromResult(entry.Open());
      }

      public Task<ITransaction> OpenTransactionAsync()
      {
         return Task.FromResult(EmptyTransaction.Instance);
      }

      public async Task WriteAsync(string id, Stream sourceStream, bool append, CancellationToken cancellationToken)
      {
         id = StoragePath.Normalize(id, false);
         ZipArchive archive = GetArchive(true);


         ZipArchiveEntry entry = archive.CreateEntry(id, CompressionLevel.Optimal);
         using (Stream dest = entry.Open())
         {
            await sourceStream.CopyToAsync(dest);
         }
      }

      public Task DeleteAsync(IEnumerable<string> ids, CancellationToken cancellationToken)
      {
         ZipArchive archive = GetArchive(true);

         foreach(string id in ids)
         {
            string nid = StoragePath.Normalize(id, false);

            ZipArchiveEntry entry = archive.GetEntry(nid);
            if (entry == null) continue;

            try
            {
               entry.Delete();
            }
            catch(NotSupportedException)
            {

            }
         }

         return Task.FromResult(true);
      }


      public Task<Stream> OpenWriteAsync(string id, bool append, CancellationToken cancellationToken)
      {
         var callbackStream = new FixedStream(new MemoryStream(), null, fx =>
         {
            id = StoragePath.Normalize(id, false);
            ZipArchive archive = GetArchive(true);

            ZipArchiveEntry entry = archive.CreateEntry(id, CompressionLevel.Optimal);
            using (Stream dest = entry.Open())
            {
               fx.Parent.Position = 0;
               fx.Parent.CopyTo(dest);
            }
         });

         return Task.FromResult<Stream>(callbackStream);
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
   }
}
