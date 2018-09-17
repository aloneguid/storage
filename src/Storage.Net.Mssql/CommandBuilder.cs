using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Text;
using Storage.Net.KeyValue;
using NetBox.Extensions;

namespace Storage.Net.Mssql
{
   class CommandBuilder
   {
      private readonly SqlConnection _sqlConnection;
      private readonly SqlConfiguration _config;

      public CommandBuilder(SqlConnection sqlConnection, SqlConfiguration config)
      {
         _sqlConnection = sqlConnection;
         _config = config;
      }

      public SqlCommand BuidInsertRowCommand(string tableName, Value row, bool isUpsert)
      {
         var s = new StringBuilder();

         if(isUpsert && row.Keys.Count > 0)
         {
            s.Append("IF EXISTS (SELECT [");
            s.Append(SqlConstants.PartitionKey);
            s.Append("] FROM [");
            s.Append(tableName);
            s.Append("]");
            AddWhereLimit(s);
            s.Append(") ");
            AddUpdate(tableName, s, row);
            s.Append(" ELSE ");
         }

         AddInsert(tableName, s, row);

         SqlCommand cmd = _sqlConnection.CreateCommand();
         cmd.CommandText = s.ToString();

         cmd.Parameters.AddWithValue("@pk", row.PartitionKey);
         cmd.Parameters.AddWithValue("@rk", row.RowKey);
         cmd.Parameters.AddWithValue("@doc", row.JsonSerialise());

         return cmd;
      }

      private void AddUpdate(string tableName, StringBuilder s, Value row)
      {
         s.Append("UPDATE [");
         s.Append(tableName);
         s.Append("] SET [");
         s.Append(SqlConstants.DocumentColumn);
         s.Append("] = @doc");

         AddWhereLimit(s);
      }

      private void AddWhereLimit(StringBuilder s)
      {
         s.Append(" WHERE [");
         s.Append(SqlConstants.PartitionKey);
         s.Append("] = @pk AND [");
         s.Append(SqlConstants.RowKey);
         s.Append("] = @rk");
      }

      private void AddInsert(string tableName, StringBuilder s, Value row)
      {
         s.Append("INSERT INTO [");
         s.Append(tableName);
         s.Append("] (");
         s.Append(SqlConstants.PartitionKey);
         s.Append(", ");
         s.Append(SqlConstants.RowKey);
         s.Append(", ");
         s.Append(SqlConstants.DocumentColumn);
         s.Append(") values (@pk, @rk, @doc)");
      }
   }
}
