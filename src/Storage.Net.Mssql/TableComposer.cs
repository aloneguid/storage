using System.Data.SqlClient;
using System.Text;
using Storage.Net.KeyValue;

namespace Storage.Net.Mssql
{
   class TableComposer
   {
      private readonly SqlConnection _connection;
      private readonly SqlConfiguration _config;

      public TableComposer(SqlConnection connection, SqlConfiguration config)
      {
         _connection = connection;
         _config = config;
      }

      public SqlCommand BuildCreateSchemaCommand(string tableName, Value row)
      {
         var s = new StringBuilder();
         s.Append("CREATE TABLE [");
         s.Append(tableName);
         s.Append("] ([");
         s.Append(SqlConstants.PartitionKey);
         s.Append("] NVARCHAR(50) NOT NULL, [");
         s.Append(SqlConstants.RowKey);
         s.Append("] NVARCHAR(50) NOT NULL, [");
         s.Append(SqlConstants.DocumentColumn);
         s.Append("] NTEXT, ");

         s.Append($"PRIMARY KEY ([{SqlConstants.PartitionKey}], [{SqlConstants.RowKey}])");
         s.Append(")");

         SqlCommand cmd = _connection.CreateCommand();
         cmd.CommandText = s.ToString();
         return cmd;
      }

   }
}
