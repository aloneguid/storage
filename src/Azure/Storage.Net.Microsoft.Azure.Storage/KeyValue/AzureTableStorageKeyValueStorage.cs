using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Table;
using NetBox.Extensions;
using Storage.Net.KeyValue;
using AzSE = Microsoft.WindowsAzure.Storage.StorageException;
using IAzTableEntity = Microsoft.WindowsAzure.Storage.Table.ITableEntity;
using MeSE = Storage.Net.StorageException;

namespace Storage.Net.Microsoft.Azure.Storage.KeyValue
{
   /// <summary>
   /// Microsoft Azure Table storage
   /// </summary>
   class AzureTableStorageKeyValueStorage : IKeyValueStorage
   {
      private const int MaxInsertLimit = 100;
      private const string PartitionKeyName = "PartitionKey";
      private const string RowKeyName = "RowKey";
      private readonly CloudTableClient _client;
      private static readonly ConcurrentDictionary<string, TableTag> TableNameToTableTag = new ConcurrentDictionary<string, TableTag>();
      private static readonly Regex TableNameRgx = new Regex("^[A-Za-z][A-Za-z0-9]{2,62}$");

      private class TableTag
      {
         public CloudTable Table { get; set; }

         public bool Exists { get; set; }
      }

      /// <summary>
      /// Creates an instance by account name and storage key
      /// </summary>
      /// <param name="accountName"></param>
      /// <param name="storageKey"></param>
      public AzureTableStorageKeyValueStorage(string accountName, string storageKey)
      {
         if(accountName == null)
            throw new ArgumentNullException(nameof(accountName));
         if(storageKey == null)
            throw new ArgumentNullException(nameof(storageKey));

         var account = new CloudStorageAccount(new StorageCredentials(accountName, storageKey), true);
         _client = account.CreateCloudTableClient();
      }

      /// <summary>
      /// For use with local development storage.
      /// </summary>
      public AzureTableStorageKeyValueStorage()
      {
         if(CloudStorageAccount.TryParse(Constants.UseDevelopmentStorageConnectionString, out CloudStorageAccount account))
         {
            _client = account.CreateCloudTableClient();
         }
         else
         {
            throw new InvalidOperationException($"Cannot connect to local development environment when creating key-value storage.");
         }
      }

      /// <summary>
      /// Returns true as Azure supports optimistic concurrency
      /// </summary>
      public bool HasOptimisticConcurrency
      {
         get { return true; }
      }

      /// <summary>
      /// Returns the list of table names in this storage
      /// </summary>
      /// <returns></returns>
      public async Task<IReadOnlyCollection<string>> ListTableNamesAsync()
      {
         var result = new List<string>();

         TableContinuationToken token = null;

         do
         {
            TableResultSegment segment = await _client.ListTablesSegmentedAsync(token);

            foreach(CloudTable table in segment.Results)
            {
               result.Add(table.Name);
            }

            token = segment.ContinuationToken;
         }
         while(token != null);

         return result;
      }

      /// <summary>
      /// Deletes table completely
      /// </summary>
      /// <param name="tableName"></param>
      public async Task DeleteAsync(string tableName)
      {
         CloudTable table = await GetTableAsync(tableName, false);
         if(table != null)
         {
            await table.DeleteAsync();
            TableNameToTableTag.TryRemove(tableName, out TableTag tag);
         }
      }

      /// <summary>
      /// Gets the list of rows in a table by partition and row key
      /// </summary>
      public async Task<IReadOnlyCollection<Value>> GetAsync(string tableName, Key key)
      {
         if(tableName == null)
            throw new ArgumentNullException(nameof(tableName));
         if(key == null)
            throw new ArgumentNullException(nameof(key));

         return await InternalGetAsync(tableName, key, -1);
      }

      private async Task<IReadOnlyCollection<Value>> InternalGetAsync(string tableName, Key key, int maxRecords)
      {
         CloudTable table = await GetTableAsync(tableName, false);
         if(table == null)
         {
            return new List<Value>();
         }

         var query = new TableQuery();

         var filters = new List<string>();

         if(key.PartitionKey != null)
         {
            filters.Add(TableQuery.GenerateFilterCondition(
               PartitionKeyName,
               QueryComparisons.Equal,
               EncodeKey(key.PartitionKey)));
         }

         if(key.RowKey != null)
         {
            filters.Add(TableQuery.GenerateFilterCondition(
               RowKeyName,
               QueryComparisons.Equal,
               EncodeKey(key.RowKey)));
         }

         if(filters.Count > 0)
         {
            string finalFilter = filters.First();

            for(int i = 1; i < filters.Count; i++)
            {
               finalFilter = TableQuery.CombineFilters(finalFilter, TableOperators.And, filters[i]);
            }

            query = query.Where(finalFilter);
         }

         if(maxRecords > 0)
         {
            query = query.Take(maxRecords);
         }

         TableContinuationToken token = null;
         var entities = new List<DynamicTableEntity>();
         do
         {
            var queryResults = await table.ExecuteQuerySegmentedAsync(query, token);
            entities.AddRange(queryResults.Results);
            token = queryResults.ContinuationToken;
         } while(token != null);

         return entities.Select(ToTableRow).ToList();
      }

      /// <summary>
      /// As per interface
      /// </summary>
      public async Task InsertAsync(string tableName, IReadOnlyCollection<Value> values)
      {
         if(tableName == null)
            throw new ArgumentNullException(nameof(tableName));
         if(values == null)
            throw new ArgumentNullException(nameof(values));

         var rowsList = values.ToList();
         if(rowsList.Count == 0)
            return;
         if(!Value.AreDistinct(rowsList))
         {
            throw new MeSE(ErrorCode.DuplicateKey, null);
         }

         await BatchedOperationAsync(tableName, true,
            (b, te) => b.Insert(te),
            rowsList);
      }

      /// <summary>
      /// See interface
      /// </summary>
      public async Task InsertOrReplaceAsync(string tableName, IReadOnlyCollection<Value> values)
      {
         if(tableName == null)
            throw new ArgumentNullException(nameof(tableName));
         if(values == null)
            throw new ArgumentNullException(nameof(values));

         var rowsList = values.ToList();
         if(rowsList.Count == 0)
            return;
         if(!Value.AreDistinct(rowsList))
         {
            throw new MeSE(ErrorCode.DuplicateKey, null);
         }

         await BatchedOperationAsync(tableName, true,
            (b, te) => b.InsertOrReplace(te),
            rowsList);
      }

      /// <summary>
      /// As per interface
      /// </summary>
      public async Task UpdateAsync(string tableName, IReadOnlyCollection<Value> values)
      {
         await BatchedOperationAsync(tableName, false,
            (b, te) => b.Replace(te),
            values);
      }

      /// <summary>
      /// As per interface
      /// </summary>
      public async Task MergeAsync(string tableName, IReadOnlyCollection<Value> values)
      {
         await BatchedOperationAsync(tableName, true,
            (b, te) => b.InsertOrMerge(te),
            values);
      }

      /// <summary>
      /// As per interface
      /// </summary>
      public async Task DeleteAsync(string tableName, IReadOnlyCollection<Key> rowIds)
      {
         if(rowIds == null)
            return;

         await BatchedOperationAsync(tableName, true,
            (b, te) => b.Delete(te),
            rowIds);
      }

      private async Task BatchedOperationAsync(string tableName, bool createTable,
         Action<TableBatchOperation, IAzTableEntity> azAction,
         IEnumerable<Value> rows)
      {
         if(tableName == null)
            throw new ArgumentNullException("tableName");
         if(rows == null)
            return;

         CloudTable table = await GetTableAsync(tableName, createTable);
         if(table == null)
            return;

         await Task.WhenAll(rows.GroupBy(e => e.PartitionKey).Select(g => BatchedOperationAsync(table, g, azAction)));
      }

      private async Task BatchedOperationAsync(CloudTable table, IGrouping<string, Value> group, Action<TableBatchOperation, IAzTableEntity> azAction)
      {
         foreach(IEnumerable<Value> chunk in group.Chunk(MaxInsertLimit))
         {
            if(chunk == null)
               break;

            var chunkLst = new List<Value>(chunk);
            var batch = new TableBatchOperation();
            foreach(Value row in chunkLst)
            {
               azAction(batch, new EntityAdapter(row));
            }

            List<TableResult> result = await ExecOrThrowAsync(table, batch);
            for(int i = 0; i < result.Count && i < chunkLst.Count; i++)
            {
               TableResult tr = result[i];
               Value row = chunkLst[i];
            }
         }
      }

      private async Task BatchedOperationAsync(string tableName, bool createTable,
         Action<TableBatchOperation, IAzTableEntity> azAction,
         IEnumerable<Key> rowIds)
      {
         if(tableName == null)
            throw new ArgumentNullException("tableName");
         if(rowIds == null)
            return;

         CloudTable table = await GetTableAsync(tableName, createTable);
         if(table == null)
            return;

         foreach(IGrouping<string, Key> group in rowIds.GroupBy(e => e.PartitionKey))
         {
            foreach(IEnumerable<Key> chunk in group.Chunk(MaxInsertLimit))
            {
               if(chunk == null)
                  break;

               var batch = new TableBatchOperation();
               foreach(Key row in chunk)
               {
                  azAction(batch, new EntityAdapter(row));
               }

               await ExecOrThrowAsync(table, batch);
            }
         }
      }

      private async Task<CloudTable> GetTableAsync(string name, bool createIfNotExists)
      {
         if(name == null)
            throw new ArgumentNullException(nameof(name));
         if(!TableNameRgx.IsMatch(name))
            throw new ArgumentException(@"invalid table name: " + name, nameof(name));

         bool cached = TableNameToTableTag.TryGetValue(name, out TableTag tag);

         if(!cached)
         {
            tag = new TableTag
            {
               Table = _client.GetTableReference(name),
            };
            tag.Exists = await tag.Table.ExistsAsync();
            TableNameToTableTag[name] = tag;
         }

         if(!tag.Exists && createIfNotExists)
         {
            await tag.Table.CreateAsync();
            tag.Exists = true;
         }

         if(!tag.Exists)
            return null;
         return tag.Table;
      }

      private async Task<List<TableResult>> ExecOrThrowAsync(CloudTable table, TableBatchOperation op)
      {
         try
         {
            return (await table.ExecuteBatchAsync(op)).ToList();
         }
         catch(AzSE ex)
         {
            if(ex.RequestInformation.HttpStatusCode == 409)
            {
               throw new MeSE(ErrorCode.DuplicateKey, ex);
            }

            throw new MeSE(ex.Message, ex);
         }
      }

      private static Value ToTableRow(DynamicTableEntity az)
      {
         var result = new Value(az.PartitionKey, az.RowKey);
         foreach(KeyValuePair<string, EntityProperty> pair in az.Properties)
         {
            switch(pair.Value.PropertyType)
            {
               case EdmType.Boolean:
                  result[pair.Key] = pair.Value.BooleanValue;
                  break;
               case EdmType.DateTime:
                  result[pair.Key] = pair.Value.DateTime.Value.ToUniversalTime();
                  break;
               case EdmType.Int32:
                  result[pair.Key] = pair.Value.Int32Value;
                  break;
               case EdmType.Int64:
                  result[pair.Key] = pair.Value.Int64Value;
                  break;
               case EdmType.Double:
                  result[pair.Key] = pair.Value.DoubleValue;
                  break;
               case EdmType.Guid:
                  result[pair.Key] = pair.Value.GuidValue;
                  break;
               case EdmType.Binary:
                  result[pair.Key] = pair.Value.BinaryValue;
                  break;
               default:
                  result[pair.Key] = pair.Value.StringValue;
                  break;
            }
         }
         return result;
      }

      private static string EncodeKey(string key)
      {
         //todo: read more: https://msdn.microsoft.com/library/azure/dd179338.aspx
         return key.UrlEncode();
      }

      /// <summary>
      /// Checks if blob name is valid
      /// </summary>
      /// <param name="id"></param>
      /// <returns></returns>
      public static bool IsValidBlobName(string id)
      {
         /*
          * A blob name must conforming to the following naming rules:
          * - A blob name can contain any combination of characters.
          * - A blob name must be at least one character long and cannot be more than 1,024 characters long.
          * - Blob names are case-sensitive.
          * - Reserved URL characters must be properly escaped.
          * - The number of path segments comprising the blob name cannot exceed 254. A path segment is the string between consecutive delimiter characters (e.g., the forward slash '/') that corresponds to the name of a virtual directory.
          */

         if(string.IsNullOrEmpty(id))
            return false;
         if(id.Length == 0)
            return false;
         if(id.Length > 1024)
            return false;
         if(id.UrlEncode() != id)
            return false;

         return true;
      }

      /// <summary>
      /// Nothing to dispose here
      /// </summary>
      public void Dispose()
      {
      }
   }
}
