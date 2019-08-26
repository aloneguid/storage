using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using NetBox;
using NetBox.Extensions;
using Storage.Net.Blobs;
using Storage.Net.Streaming;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Storage.Net.Microsoft.ServiceFabric.Blobs
{
   class ServiceFabricReliableDictionaryBlobStorageProvider : IBlobStorage
   {
      private readonly IReliableStateManager _stateManager;
      private readonly string _collectionName;
      private ServiceFabricTransaction _currentTransaction;

      public ServiceFabricReliableDictionaryBlobStorageProvider(IReliableStateManager stateManager, string collectionName)
      {
         _stateManager = stateManager ?? throw new ArgumentNullException(nameof(stateManager));
         _collectionName = collectionName ?? throw new ArgumentNullException(nameof(collectionName));
      }

      public async Task<IReadOnlyCollection<Blob>> ListAsync(ListOptions options, CancellationToken cancellationToken)
      {
         if (options == null) options = new ListOptions();

         var result = new List<Blob>();

         using (ServiceFabricTransaction tx = GetTransaction())
         {
            IReliableDictionary<string, byte[]> coll = await OpenCollectionAsync();

            IAsyncEnumerable<KeyValuePair<string, byte[]>> enumerable =
               await coll.CreateEnumerableAsync(tx.Tx);

            using (IAsyncEnumerator<KeyValuePair<string, byte[]>> enumerator = enumerable.GetAsyncEnumerator())
            {
               while (await enumerator.MoveNextAsync(cancellationToken))
               {
                  KeyValuePair<string, byte[]> current = enumerator.Current;

                  if (options.FilePrefix == null || current.Key.StartsWith(options.FilePrefix))
                  {
                     result.Add(new Blob(current.Key, BlobItemKind.File));
                  }
               }
            }
         }

         return result;
      }

      public Task<Stream> OpenWriteAsync(string fullPath, bool append, CancellationToken cancellationToken = default)
      {
         GenericValidation.CheckBlobFullPath(fullPath);

         return Task.FromResult<Stream>(new FixedStream(new MemoryStream(), null, async fx =>
         {
            fx.Parent.Position = 0;

            if(append)
            {
               await AppendAsync(fullPath, fx.Parent);
            }
            else
            {
               await WriteAsync(fullPath, fx.Parent);
            }
         }));
      }

      private async Task WriteAsync(Blob blob, Stream sourceStream)
      {
         string fullPath = ToFullPath(blob);

         byte[] value = sourceStream.ToByteArray();

         using (ServiceFabricTransaction tx = GetTransaction())
         {
            IReliableDictionary<string, byte[]> coll = await OpenCollectionAsync();
            IReliableDictionary<string, BlobMetaTag> metaColl = await OpenMetaCollectionAsync();

            var meta = new BlobMetaTag
            {
               LastModificationTime = DateTimeOffset.UtcNow,
               Length = value.LongLength,
               Md = value.GetHash(HashType.Md5).ToHexString()
            };

            await metaColl.AddOrUpdateAsync(tx.Tx, fullPath, meta, (k, v) => meta);
            await coll.AddOrUpdateAsync(tx.Tx, fullPath, value, (k, v) => value);

            await tx.CommitAsync();
         }
      }

      private async Task AppendAsync(string fullPath, Stream sourceStream)
      {
         fullPath = ToFullPath(fullPath);

         using (ServiceFabricTransaction tx = GetTransaction())
         {
            IReliableDictionary<string, byte[]> coll = await OpenCollectionAsync();

            //create a new byte array with
            byte[] extra = sourceStream.ToByteArray();
            ConditionalValue<byte[]> value = await coll.TryGetValueAsync(tx.Tx, fullPath);
            int oldLength = value.HasValue ? value.Value.Length : 0;
            byte[] newData = new byte[oldLength + extra.Length];
            if(value.HasValue)
            {
               Array.Copy(value.Value, newData, oldLength);
            }
            Array.Copy(extra, 0, newData, oldLength, extra.Length);

            //put new array into the key
            await coll.AddOrUpdateAsync(tx.Tx, fullPath, extra, (k, v) => extra);

            //commit the transaction
            await tx.CommitAsync();
         }
      }

      public async Task<Stream> OpenReadAsync(string id, CancellationToken cancellationToken)
      {
         GenericValidation.CheckBlobFullPath(id);
         id = ToFullPath(id);

         using (ServiceFabricTransaction tx = GetTransaction())
         {
            IReliableDictionary<string, byte[]> coll = await OpenCollectionAsync();
            ConditionalValue<byte[]> value = await coll.TryGetValueAsync(tx.Tx, id);

            if (!value.HasValue) throw new StorageException(ErrorCode.NotFound, null);

            return new MemoryStream(value.Value);
         }
      }

      public async Task DeleteAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken)
      {
         GenericValidation.CheckBlobFullPaths(fullPaths);

         using (ServiceFabricTransaction tx = GetTransaction())
         {
            IReliableDictionary<string, byte[]> coll = await OpenCollectionAsync();

            foreach (string fullPath in fullPaths)
            {
               await coll.TryRemoveAsync(tx.Tx, ToFullPath(fullPath));
            }

            await tx.CommitAsync();
         }
      }

      public async Task<IReadOnlyCollection<bool>> ExistsAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken)
      {
         GenericValidation.CheckBlobFullPaths(fullPaths);

         var result = new List<bool>();
         using (ServiceFabricTransaction tx = GetTransaction())
         {
            IReliableDictionary<string, byte[]> coll = await OpenCollectionAsync();

            foreach (string fullPath in fullPaths)
            {
               bool exists = await coll.ContainsKeyAsync(tx.Tx, ToFullPath(fullPath));

               result.Add(exists);
            }
         }
         return result;
      }

      public async Task<IReadOnlyCollection<Blob>> GetBlobsAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken)
      {
         GenericValidation.CheckBlobFullPaths(fullPaths);

         var result = new List<Blob>();

         using (ServiceFabricTransaction tx = GetTransaction())
         {
            IReliableDictionary<string, byte[]> coll = await OpenCollectionAsync();

            foreach (string fullPath in fullPaths)
            {
               ConditionalValue<byte[]> value = await coll.TryGetValueAsync(tx.Tx, ToFullPath(fullPath));

               if (!value.HasValue)
               {
                  result.Add(null);
               }
               else
               {
                  var meta = new Blob(fullPath)
                  {
                     Size = value.Value.Length
                  };
                  result.Add(meta);
               }
            }
         }
         return result;
      }

      public Task SetBlobsAsync(IEnumerable<Blob> blobs, CancellationToken cancellationToken = default)
      {
         throw new NotSupportedException();
      }

      private async Task<IReliableDictionary<string, byte[]>> OpenCollectionAsync()
      {
         IReliableDictionary<string, byte[]> collection = 
            await _stateManager.GetOrAddAsync<IReliableDictionary<string, byte[]>>(_collectionName);

         return collection;
      }

      private async Task<IReliableDictionary<string, BlobMetaTag>> OpenMetaCollectionAsync()
      {
         IReliableDictionary<string, BlobMetaTag> collection =
            await _stateManager.GetOrAddAsync<IReliableDictionary<string, BlobMetaTag>>(_collectionName + "_meta");

         return collection;
      }

      public void Dispose()
      {
      }

      private ServiceFabricTransaction GetTransaction()
      {
         if (_currentTransaction != null) return new ServiceFabricTransaction(_currentTransaction);

         return new ServiceFabricTransaction(_stateManager, null);
      }

      public Task<ITransaction> OpenTransactionAsync()
      {
         if (_currentTransaction != null)
            throw new InvalidOperationException($"transaction already open");

         _currentTransaction = new ServiceFabricTransaction(_stateManager, CloseTransaction);

         return Task.FromResult<ITransaction>(_currentTransaction);
      }

      private void CloseTransaction(bool b)
      {
         //dispose on transaction is already called!
         _currentTransaction = null;
      }

      private string ToFullPath(string fullPath)
      {
         return StoragePath.Normalize(fullPath, false);
      }

      public Task MoveBlobAsync(string fromPath, string toPath, CancellationToken cancellationToken = default)
      {
         throw new NotImplementedException();
      }
   }
}
