using System.Collections.Generic;
using System.IO;
using System;
using NetBox.Model;
using System.Linq;
using NetBox.IO;
using System.Threading.Tasks;
using System.Threading;
using NetBox.Extensions;
using NetBox;
using Storage.Net.Streaming;

namespace Storage.Net.Blob
{
   class InMemoryBlobStorage : IBlobStorage
   {
      struct Tag
      {
         public byte[] data;
         public DateTimeOffset lastMod;
         public string md5;
      }

      private readonly Dictionary<BlobId, Tag> _idToData = new Dictionary<BlobId, Tag>();

      public Task<IReadOnlyCollection<BlobId>> ListAsync(ListOptions options, CancellationToken cancellationToken)
      {
         if (options == null) options = new ListOptions();

         options.FolderPath = StoragePath.Normalize(options.FolderPath);

         List<BlobId> matches = _idToData

            .Where(e => options.Recurse
               ? e.Key.FolderPath.StartsWith(options.FolderPath)
               : e.Key.FolderPath == options.FolderPath)

            .Select(e => e.Key)
            .Where(options.IsMatch)
            .Where(e => options.BrowseFilter == null || options.BrowseFilter(e))
            .Take(options.MaxResults == null ? int.MaxValue : options.MaxResults.Value)
            .ToList();

         return Task.FromResult((IReadOnlyCollection<BlobId>)matches);
      }

      public Task WriteAsync(string id, Stream sourceStream, bool append, CancellationToken cancellationToken)
      {
         GenericValidation.CheckBlobId(id);

         if (append)
         {
            if (!Exists(id))
            {
               Write(id, sourceStream);
            }
            else
            {
               Tag tag = _idToData[id];
               byte[] data = tag.data.Concat(sourceStream.ToByteArray()).ToArray();

               _idToData[id] = ToTag(data);
            }
         }
         else
         {
            Write(id, sourceStream);
         }

         return Task.FromResult(true);
      }

      public Task<Stream> OpenWriteAsync(string id, bool append, CancellationToken cancellationToken)
      {
         GenericValidation.CheckBlobId(id);

         var result = new FixedStream(new MemoryStream(), null, fx =>
         {
            MemoryStream ms = (MemoryStream)fx.Parent;
            ms.Position = 0;
            WriteAsync(id, ms, append, cancellationToken).Wait();
         });

         return Task.FromResult<Stream>(result);
      }

      public Task<Stream> OpenReadAsync(string id, CancellationToken cancellationToken)
      {
         GenericValidation.CheckBlobId(id);

         if (!_idToData.TryGetValue(id, out Tag tag)) return Task.FromResult<Stream>(null);

         return Task.FromResult<Stream>(new NonCloseableStream(new MemoryStream(tag.data)));
      }

      public Task DeleteAsync(IEnumerable<string> ids, CancellationToken cancellationToken)
      {
         GenericValidation.CheckBlobId(ids);

         foreach (string blobId in ids)
         {
            _idToData.Remove(blobId);
         }

         return Task.FromResult(true);
      }

      public Task<IReadOnlyCollection<bool>> ExistsAsync(IEnumerable<string> ids, CancellationToken cancellationToken)
      {
         var result = new List<bool>();

         foreach (string id in ids)
         {
            result.Add(_idToData.ContainsKey(id));
         }

         return Task.FromResult<IReadOnlyCollection<bool>>(result);
      }

      public Task<IEnumerable<BlobMeta>> GetMetaAsync(IEnumerable<string> ids, CancellationToken cancellationToken)
      {
         GenericValidation.CheckBlobId(ids);

         var result = new List<BlobMeta>();

         foreach (string id in ids)
         {
            if (!_idToData.TryGetValue(id, out Tag tag))
            {
               result.Add(null);
            }
            else
            {
               var meta = new BlobMeta(tag.data.Length, tag.md5, tag.lastMod);

               result.Add(meta);
            }
         }

         return Task.FromResult<IEnumerable<BlobMeta>>(result);
      }

      private void Write(string id, Stream sourceStream)
      {
         Tag tag = ToTag(sourceStream);

         _idToData[id] = tag;
      }

      private static Tag ToTag(Stream s)
      {
         if (s is MemoryStream ms) ms.Position = 0;
         return ToTag(s.ToByteArray());
      }

      private static Tag ToTag(byte[] data)
      {
         var tag = new Tag();
         tag.data = data;
         tag.lastMod = DateTimeOffset.UtcNow;
         tag.md5 = tag.data.GetHash(HashType.Md5).ToHexString();
         return tag;
      }

      private bool Exists(string id)
      {
         GenericValidation.CheckBlobId(id);

         return _idToData.ContainsKey(id);
      }

      public void Dispose()
      {
      }

      public Task<ITransaction> OpenTransactionAsync()
      {
         return Task.FromResult(EmptyTransaction.Instance);
      }
   }
}
