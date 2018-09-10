using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NetBox;
using NetBox.Data;
using Storage.Net.KeyValue;

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

      public async Task ExecAsync(string tableName, List<Tuple<SqlCommand, TableRow>> commands)
      {
         await CheckConnection();

         var sample = commands.Select(c => c.Item2).ToList();

         using (SqlTransaction tx = _sqlConnection.BeginTransaction())
         {
            foreach (Tuple<SqlCommand, TableRow> cmd in commands)
            {
               await ExecAsync(tableName, cmd.Item1, sample, tx);
            }

            tx.Commit();
         }
      }

      public async Task ExecAsync(string tableName, SqlCommand cmd, IEnumerable<TableRow> rows, SqlTransaction transaction = null)
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

      private async Task CreateTable(string tableName, IEnumerable<TableRow> rows, SqlTransaction transaction = null)
      {
         TableRow masterRow = TableRow.Merge(rows);

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

      public async Task<ICollection<TableRow>> ExecRowsAsync(string sql, params object[] parameters)
      {
         var result = new List<TableRow>();

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

      private TableRow CreateRow(SqlDataReader reader)
      {
         TableRow row = CreateMinRow(reader, out int colsUsed);

         for(int i = colsUsed; i < reader.FieldCount; i++)
         {
            string name = reader.GetName(i);
            object value = reader[i];
            row[name] = new DynamicValue(value);
         }

         return row;
      }

      private TableRow CreateMinRow(SqlDataReader reader, out int colsUsed)
      {
         string partitionKey = (reader.FieldCount > 0 && reader.GetName(0) == _config.PartitionKeyColumnName)
            ? reader[_config.PartitionKeyColumnName] as string
            : null;

         string rowKey = (reader.FieldCount > 1 && reader.GetName(1) == _config.RowKeyColumnName)
            ? reader[_config.RowKeyColumnName] as string
            : null;

         colsUsed = (partitionKey == null ? 0 : 1) + (rowKey == null ? 0 : 1);

         return new TableRow(partitionKey ?? "none", rowKey ?? "none");
      }
   }
}
