﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using Storage.Net.KeyValue;

namespace Storage.Net.Mssql
{
   public class MssqlKeyValueStorage : IKeyValueStorage
   {
      private readonly SqlConnection _connection;
      private readonly CommandBuilder _cb;
      private readonly CommandExecutor _exec;
      private readonly SqlConfiguration _config;

      public MssqlKeyValueStorage(string connectionString, SqlConfiguration config)
      {
         _config = config ?? new SqlConfiguration();
         _connection = new SqlConnection(connectionString);
         _cb = new CommandBuilder(_connection, _config);
         _exec = new CommandExecutor(_connection, _config);
      }

      public bool HasOptimisticConcurrency => false;

      public async Task DeleteAsync(string tableName)
      {
         if (tableName == null)
         {
            throw new ArgumentNullException(nameof(tableName));
         }

         try
         {
            await _exec.ExecAsync($"DROP TABLE [{tableName}]");
         }
         catch(SqlException ex) when (ex.Number == 3701)
         {
         }
      }

      public async Task DeleteAsync(string tableName, IEnumerable<Key> rowIds)
      {
         if (rowIds == null) return;

         foreach(Key id in rowIds)
         {
            await _exec.ExecAsync("DELETE FROM [{0}] where [{1}] = '{2}' AND [{3}] = '{4}'",
               tableName,
               SqlConstants.PartitionKey, id.PartitionKey,
               SqlConstants.RowKey, id.RowKey);
         }
      }

      public void Dispose()
      {
         _connection.Dispose();
      }

      public async Task<IReadOnlyCollection<Value>> GetAsync(string tableName, string partitionKey)
      {
         if (tableName == null) throw new ArgumentNullException(nameof(tableName));
         if (partitionKey == null) throw new ArgumentNullException(nameof(partitionKey));

         return await InternalGetAsync(tableName, partitionKey, null);
      }

      public async Task<Value> GetAsync(string tableName, string partitionKey, string rowKey)
      {
         if (tableName == null) throw new ArgumentNullException(nameof(tableName));
         if (partitionKey == null) throw new ArgumentNullException(nameof(partitionKey));
         if (rowKey == null) throw new ArgumentNullException(nameof(rowKey));

         return (await InternalGetAsync(tableName, partitionKey, rowKey)).FirstOrDefault();
      }

      private async Task<IReadOnlyCollection<Value>> InternalGetAsync(string tableName, string partitionKey, string rowKey)
      {
         string sql = $"SELECT * FROM [{tableName}] WHERE [{SqlConstants.PartitionKey}] = '{partitionKey}'";
         if (rowKey != null) sql += $" AND [{SqlConstants.RowKey}] = '{rowKey}'";
         return await _exec.ExecRowsAsync(sql);
      }

      public async Task InsertAsync(string tableName, IEnumerable<Value> rows)
      {
         if (tableName == null) throw new ArgumentNullException(nameof(tableName));

#if NETFULL
         if(rows.Count() > _config.UseBulkCopyOnBatchesGreaterThan)
         {
            var sbc = new BulkCopyExecutor(_connection, _config, tableName);
            await sbc.InsertAsync(rows);
         }
         else
         {
            await Exec(tableName, rows, false);
         }
#else
         await Exec(tableName, rows, false);
#endif
      }

      public async Task InsertOrReplaceAsync(string tableName, IEnumerable<Value> rows)
      {
         if (tableName == null) throw new ArgumentNullException(nameof(tableName));

         await Exec(tableName, rows, true);
      }

      public async Task<IReadOnlyCollection<string>> ListTableNamesAsync()
      {
         string sql = "SELECT TABLE_NAME FROM {0}.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'";

         IReadOnlyCollection<Value> rows = await _exec.ExecRowsAsync(sql, _connection.Database);

         return rows.Select(r => (string)r[r.Keys.First()]).ToList();
      }

      public Task MergeAsync(string tableName, IEnumerable<Value> rows)
      {
         if (tableName == null) throw new ArgumentNullException(nameof(tableName));

         throw new NotImplementedException();
      }

      public Task UpdateAsync(string tableName, IEnumerable<Value> rows)
      {
         if (tableName == null) throw new ArgumentNullException(nameof(tableName));

         throw new NotImplementedException();
      }

      private async Task Exec(string tableName, IEnumerable<Value> rows, bool isUpsert)
      {
         List<Tuple<SqlCommand, Value>> commands = rows.Select(r => Tuple.Create(_cb.BuidInsertRowCommand(tableName, r, isUpsert), r)).ToList();

         await _exec.ExecAsync(tableName, commands);
      }
   }
}
