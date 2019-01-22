using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using Storage.Net.KeyValue;
using NetBox.Extensions;

namespace Storage.Net.Mssql
{
   class CommandExecutor
   {
      private readonly SqlConnection _sqlConnection;
      private readonly SqlConfiguration _config;

      public CommandExecutor(SqlConnection sqlConnection, SqlConfiguration config)
      {
         _sqlConnection = sqlConnection ?? throw new ArgumentNullException(nameof(sqlConnection));
         _config = config;
      }

      private async Task ExecAsync(SqlCommand cmd, SqlTransaction transaction = null)
      {
         await CheckConnection();

         cmd.Transaction = transaction;
         await cmd.ExecuteNonQueryAsync();
      }

      public async Task ExecAsync(string tableName, List<Tuple<SqlCommand, Value>> commands)
      {
         await CheckConnection();

         var sample = commands.Select(c => c.Item2).ToList();

         using (SqlTransaction tx = _sqlConnection.BeginTransaction())
         {
            foreach (Tuple<SqlCommand, Value> cmd in commands)
            {
               await ExecAsync(tableName, cmd.Item1, sample, tx);
            }

            tx.Commit();
         }
      }

      public async Task ExecAsync(string tableName, SqlCommand cmd, IEnumerable<Value> rows, SqlTransaction transaction = null)
      {
         try
         {
            await ExecAsync(cmd, transaction);
         }
         catch (SqlException ex) when (ex.Number == SqlCodes.InvalidObjectName)
         {
            await CreateTable(tableName, rows, transaction);

            await ExecAsync(cmd, transaction);
         }
         catch (SqlException ex) when (ex.Number == SqlCodes.DuplicateKey)
         {
            throw new StorageException(ErrorCode.DuplicateKey, ex);
         }

      }

      private async Task CreateTable(string tableName, IEnumerable<Value> rows, SqlTransaction transaction = null)
      {
         Value masterRow = Value.Merge(rows);

         var composer = new TableComposer(_sqlConnection, _config);
         SqlCommand cmd = composer.BuildCreateSchemaCommand(tableName, masterRow);
         await ExecAsync(cmd, transaction);
      }

      public async Task ExecAsync(string sql, params object[] parameters)
      {
         SqlCommand cmd = _sqlConnection.CreateCommand();
         cmd.CommandText = string.Format(sql, parameters);
         await ExecAsync(cmd);
      }

      public async Task<IReadOnlyCollection<Value>> ExecRowsAsync(string sql, params object[] parameters)
      {
         var result = new List<Value>();

         using (SqlCommand cmd = _sqlConnection.CreateCommand())
         {
            cmd.CommandText = string.Format(sql, parameters);

            await CheckConnection();

            try
            {
               using (SqlDataReader reader = await cmd.ExecuteReaderAsync())
               {
                  while (await reader.ReadAsync())
                  {
                     result.Add(CreateRow(reader));
                  }
               }
            }
            catch(SqlException ex) when (ex.Number == SqlCodes.InvalidObjectName)
            {
            }
         }

         return result;
      }

      public async Task CheckConnection()
      {
         if(_sqlConnection.State != ConnectionState.Open)
         {
            await _sqlConnection.OpenAsync();
         }
      }

      private Value CreateRow(SqlDataReader reader)
      {
         Value row = CreateMinRow(reader, out int colsUsed);

         for(int i = colsUsed; i < reader.FieldCount; i++)
         {
            string name = reader.GetName(i);
            object value = reader[i];
            row[name] = value;
         }

         return row;
      }

      private Value CreateMinRow(SqlDataReader reader, out int colsUsed)
      {
         string partitionKey = (reader.FieldCount > 0 && reader.GetName(0) == SqlConstants.PartitionKey)
            ? reader[SqlConstants.PartitionKey] as string
            : null;

         string rowKey = (reader.FieldCount > 1 && reader.GetName(1) == SqlConstants.RowKey)
            ? reader[SqlConstants.RowKey] as string
            : null;

         string document = (reader.FieldCount > 2 && reader.GetName(2) == SqlConstants.DocumentColumn)
            ? reader[SqlConstants.DocumentColumn] as string
            : null;

         colsUsed = (partitionKey == null ? 0 : 1) + (rowKey == null ? 0 : 1);

         var value = new Value(partitionKey ?? "none", rowKey ?? "none");

         if (document != null)
         {
            IDictionary<string, object> dic = document.JsonDeserialiseDictionary();
            foreach (KeyValuePair<string, object> kvp in dic)
            {
               value[kvp.Key] = ToStandard(kvp.Value);
            }
         }

         return value;
      }

      private object ToStandard(object value)
      {
         if (value == null)
            return null;

         if (value is DateTime dt)
            return dt.ToUniversalTime();

         if (value is DateTimeOffset dto)
            return dto.ToUniversalTime();

         return value;
      }
   }
}
