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
            IReliableDictionary<string, byte[]> coll = await OpenCollectionAsync().ConfigureAwait(false);

            global::Microsoft.ServiceFabric.Data.IAsyncEnumerable<KeyValuePair<string, byte[]>> enumerable =
               await coll.CreateEnumerableAsync(tx.Tx).ConfigureAwait(false);

            using (global::Microsoft.ServiceFabric.Data.IAsyncEnumerator<KeyValuePair<string, byte[]>> enumerator = enumerable.GetAsyncEnumerator())
            {
               while (await enumerator.MoveNextAsync(cancellationToken).ConfigureAwait(false))
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

      public async Task WriteAsync(string fullPath, Stream dataStream,
         bool append, CancellationToken cancellationToken = default)
      {
         GenericValidation.CheckBlobFullPath(fullPath);

         if(append)
         {
            await AppendAsync(fullPath, dataStream, cancellationToken).ConfigureAwait(false);
         }
         else
         {
            await WriteAsync(fullPath, dataStream, cancellationToken).ConfigureAwait(false);
         }
      }

      private async Task WriteAsync(Blob blob, Stream sourceStream, CancellationToken cancellationToken)
      {
         string fullPath = ToFullPath(blob);

         byte[] value = sourceStream.ToByteArray();

         using (ServiceFabricTransaction tx = GetTransaction())
         {
            IReliableDictionary<string, byte[]> coll = await OpenCollectionAsync().ConfigureAwait(false);
            IReliableDictionary<string, BlobMetaTag> metaColl = await OpenMetaCollectionAsync().ConfigureAwait(false);

            var meta = new BlobMetaTag
            {
               LastModificationTime = DateTimeOffset.UtcNow,
               Length = value.LongLength,
               Md = value.GetHash(HashType.Md5).ToHexString()
            };

            await metaColl.AddOrUpdateAsync(tx.Tx, fullPath, meta, (k, v) => meta).ConfigureAwait(false);
            await coll.AddOrUpdateAsync(tx.Tx, fullPath, value, (k, v) => value).ConfigureAwait(false);

            await tx.CommitAsync().ConfigureAwait(false);
         }
      }

      private async Task AppendAsync(string fullPath, Stream sourceStream, CancellationToken cancellationToken)
      {
         fullPath = ToFullPath(fullPath);

         using (ServiceFabricTransaction tx = GetTransaction())
         {
            IReliableDictionary<string, byte[]> coll = await OpenCollectionAsync().ConfigureAwait(false);

            //create a new byte array with
            byte[] extra = sourceStream.ToByteArray();
            ConditionalValue<byte[]> value = await coll.TryGetValueAsync(tx.Tx, fullPath).ConfigureAwait(false);
            int oldLength = value.HasValue ? value.Value.Length : 0;
            byte[] newData = new byte[oldLength + extra.Length];
            if(value.HasValue)
            {
               Array.Copy(value.Value, newData, oldLength);
            }
            Array.Copy(extra, 0, newData, oldLength, extra.Length);

            //put new array into the key
            await coll.AddOrUpdateAsync(tx.Tx, fullPath, extra, (k, v) => extra).ConfigureAwait(false);

            //commit the transaction
            await tx.CommitAsync().ConfigureAwait(false);
         }
      }

      public async Task<Stream> OpenReadAsync(string id, CancellationToken cancellationToken)
      {
         GenericValidation.CheckBlobFullPath(id);
         id = ToFullPath(id);

         using (ServiceFabricTransaction tx = GetTransaction())
         {
            IReliableDictionary<string, byte[]> coll = await OpenCollectionAsync().ConfigureAwait(false);
            ConditionalValue<byte[]> value = await coll.TryGetValueAsync(tx.Tx, id).ConfigureAwait(false);

            if (!value.HasValue) throw new StorageException(ErrorCode.NotFound, null);

            return new MemoryStream(value.Value);
         }
      }

      public async Task DeleteAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken)
      {
         GenericValidation.CheckBlobFullPaths(fullPaths);

         using (ServiceFabricTransaction tx = GetTransaction())
         {
            IReliableDictionary<string, byte[]> coll = await OpenCollectionAsync().ConfigureAwait(false);

            foreach (string fullPath in fullPaths)
            {
               await coll.TryRemoveAsync(tx.Tx, ToFullPath(fullPath)).ConfigureAwait(false);
            }

            await tx.CommitAsync().ConfigureAwait(false);
         }
      }

      public async Task<IReadOnlyCollection<bool>> ExistsAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken)
      {
         GenericValidation.CheckBlobFullPaths(fullPaths);

         var result = new List<bool>();
         using (ServiceFabricTransaction tx = GetTransaction())
         {
            IReliableDictionary<string, byte[]> coll = await OpenCollectionAsync().ConfigureAwait(false);

            foreach (string fullPath in fullPaths)
            {
               bool exists = await coll.ContainsKeyAsync(tx.Tx, ToFullPath(fullPath)).ConfigureAwait(false);

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
            IReliableDictionary<string, byte[]> coll = await OpenCollectionAsync().ConfigureAwait(false);

            foreach (string fullPath in fullPaths)
            {
               ConditionalValue<byte[]> value = await coll.TryGetValueAsync(tx.Tx, ToFullPath(fullPath)).ConfigureAwait(false);

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
            await _stateManager.GetOrAddAsync<IReliableDictionary<string, byte[]>>(_collectionName).ConfigureAwait(false);

         return collection;
      }

      private async Task<IReliableDictionary<string, BlobMetaTag>> OpenMetaCollectionAsync()
      {
         IReliableDictionary<string, BlobMetaTag> collection =
            await _stateManager.GetOrAddAsync<IReliableDictionary<string, BlobMetaTag>>(_collectionName + "_meta").ConfigureAwait(false);

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
         return StoragePath.Normalize(fullPath);
      }
   }
}
