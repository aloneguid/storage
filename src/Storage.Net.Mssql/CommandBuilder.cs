using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Text;
using Storage.Net.KeyValue;

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

      public SqlCommand BuidInsertRowCommand(string tableName, TableRow row, bool isUpsert)
      {
         var s = new StringBuilder();

         if(isUpsert && row.Keys.Count > 0)
         {
            s.Append("IF EXISTS (SELECT [");
            s.Append(_config.PartitionKeyColumnName);
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

         int c = 0;
         foreach(KeyValuePair<string, object> cell in row)
         {
            string pn = $"@c{c++}";
            cmd.Parameters.AddWithValue(pn, cell.Value);
         }

         return cmd;
      }

      private void AddUpdate(string tableName, StringBuilder s, TableRow row)
      {
         s.Append("UPDATE [");
         s.Append(tableName);
         s.Append("] SET ");

         bool first = true;
         int i = 0;
         foreach(KeyValuePair<string, object> cell in row)
         {
            if(first)
            {
               first = false;
            }
            else
            {
               s.Append(", ");
            }

            s.Append("[");
            s.Append(cell.Key);
            s.Append("] = ");
            s.Append("@c");
            s.Append(i++);
         }

         AddWhereLimit(s);
      }

      private void AddWhereLimit(StringBuilder s)
      {
         s.Append(" WHERE [");
         s.Append(_config.PartitionKeyColumnName);
         s.Append("] = @pk AND [");
         s.Append(_config.RowKeyColumnName);
         s.Append("] = @rk");
      }

      private void AddInsert(string tableName, StringBuilder s, TableRow row)
      {
         s.Append("INSERT INTO [");
         s.Append(tableName);
         s.Append("] (");
         s.Append(_config.PartitionKeyColumnName);
         s.Append(", ");
         s.Append(_config.RowKeyColumnName);

         foreach (KeyValuePair<string, object> cell in row)
         {
            s.Append(", [");
            s.Append(cell.Key);
            s.Append("]");
         }

         s.Append(") values (@pk, @rk");
         for (int i = 0; i < row.Count; i++)
         {
            s.Append(", @c");
            s.Append(i);
         }

         s.Append(")");
      }
   }
}
