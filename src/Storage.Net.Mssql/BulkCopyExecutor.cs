#if NETFULL
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NetBox;
using NetBox.Data;
using Storage.Net.KeyValue;

namespace Storage.Net.Mssql
{
   class BulkCopyExecutor
   {
      private readonly SqlConnection _connection;
      private readonly SqlConfiguration _configuration;
      private readonly string _tableName;

      public BulkCopyExecutor(SqlConnection connection, SqlConfiguration configuration, string tableName)
      {
         _connection = connection;
         _configuration = configuration;
         _tableName = tableName;
      }

      public async Task InsertAsync(IEnumerable<Value> rows)
      {
         List<Value> rowsList = rows.ToList();

         using (var sbc = new SqlBulkCopy(_connection))
         {
            sbc.DestinationTableName = _tableName;
            sbc.BulkCopyTimeout = (int)_configuration.BulkCopyTimeout.TotalSeconds;

            var dataTable = new DataTable(_tableName);
            AddColumns(dataTable, rowsList);

            //copy to rows
            foreach (Value row in rows)
            {
               DataRow dataRow = dataTable.NewRow();

               dataRow[SqlConstants.PartitionKey] = row.PartitionKey;
               dataRow[SqlConstants.RowKey] = row.RowKey;

               foreach (string key in row.Keys.OrderBy(kv => kv))
               {
                  row.TryGetValue("key", out object value);
                  dataRow[key] = value;
               }

               dataTable.Rows.Add(dataRow);
            }

            //execute bulk copy
            await CheckConnection();

            try
            {
               await sbc.WriteToServerAsync(dataTable);
            }
            catch(InvalidOperationException ex) when (ExceptonTranslator.GetSqlException(ex) != null)
            {
               SqlException sqlEx = ExceptonTranslator.GetSqlException(ex);

               if(sqlEx.Number == SqlCodes.InvalidObjectName)
               {
                  await CreateTableAsync(rowsList);

                  //run it again
                  await sbc.WriteToServerAsync(dataTable);
               }
            }
            catch(SqlException ex) when (ex.Number == SqlCodes.DuplicateKey)
            {
               throw new StorageException(ErrorCode.DuplicateKey, ex);
            }
         }
      }

      private static object CleanValue(DynamicValue dv)
      {
         if (dv == null || dv.OriginalValue == null) return null;

         if(dv.OriginalType == typeof(string) && string.IsNullOrEmpty((string)dv.OriginalValue))
         {
            return null;
         }

         return dv.OriginalValue;
      }

      private async Task CreateTableAsync(List<Value> rowsList)
      {
         var composer = new TableComposer(_connection, _configuration);
         SqlCommand cmd = composer.BuildCreateSchemaCommand(_tableName, Value.Merge(rowsList));
         await CheckConnection();
         await cmd.ExecuteNonQueryAsync();
      }

      private void AddColumns(DataTable dataTable, IReadOnlyCollection<Value> rows)
      {
         Value schemaRow = Value.Merge(rows);

         dataTable.Columns.Add(SqlConstants.PartitionKey);
         dataTable.Columns.Add(SqlConstants.RowKey);

         foreach (string key in schemaRow.Keys.OrderBy(kv => kv))
         {
            dataTable.Columns.Add(key);
         }
      }

      private async Task CheckConnection()
      {
         if (_connection.State != ConnectionState.Open)
         {
            await _connection.OpenAsync();
         }
      }


   }
}
#endif