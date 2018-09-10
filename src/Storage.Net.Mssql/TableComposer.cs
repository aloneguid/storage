using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Text;
using NetBox;
using NetBox.Data;
using Storage.Net.KeyValue;

namespace Storage.Net.Mssql
{
   class TableComposer
   {
      private static readonly Dictionary<Type, string> TypeToSqlTypeName = new Dictionary<Type, string>
      {
         [typeof(bool)] = "BIT",
         [typeof(DateTime)] = "DATETIME",
         [typeof(DateTimeOffset)] = "DATETIMEOFFSET",
         [typeof(int)] = "INT",
         [typeof(long)] = "BIGINT",
         [typeof(double)] = "FLOAT",
         [typeof(Guid)] = "UNIQUEIDENTIFIER",
         [typeof(decimal)] = "DECIMAL(18, 0)",
         [typeof(TimeSpan)] = "TIME(7)"
      };
      private readonly SqlConnection _connection;
      private readonly SqlConfiguration _config;

      public TableComposer(SqlConnection connection, SqlConfiguration config)
      {
         _connection = connection;
         _config = config;
      }

      public SqlCommand BuildCreateSchemaCommand(string tableName, TableRow row)
      {
         var s = new StringBuilder();
         s.Append("CREATE TABLE [");
         s.Append(tableName);
         s.Append("] ([");
         s.Append(_config.PartitionKeyColumnName);
         s.Append("] NVARCHAR(50) NOT NULL, [");
         s.Append(_config.RowKeyColumnName);
         s.Append("] NVARCHAR(50) NOT NULL, ");

         foreach (KeyValuePair<string, DynamicValue> cell in row)
         {
            Type t = cell.Value.OriginalType;

            if (!TypeToSqlTypeName.TryGetValue(t, out string typeName))
            {
               int length = cell.Value.OriginalValue == null ? 0 : cell.Value.OriginalValue.ToString().Length;
               length *= 2;
               string sLength = length == 0 ? "MAX" : length.ToString();

               typeName = $"NVARCHAR({sLength})";
            }

            s.Append("[");
            s.Append(cell.Key);
            s.Append("] ");
            s.Append(typeName);
            s.Append(" NULL, ");
         }

         s.Append($"PRIMARY KEY ([{_config.PartitionKeyColumnName}], [{_config.RowKeyColumnName}])");
         s.Append(")");

         SqlCommand cmd = _connection.CreateCommand();
         cmd.CommandText = s.ToString();
         return cmd;
      }

   }
}
