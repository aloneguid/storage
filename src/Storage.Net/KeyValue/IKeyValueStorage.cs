using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Storage.Net.KeyValue
{
   /// <summary>
   /// Common interface for working with table storage
   /// </summary>
   public interface IKeyValueStorage : IDisposable
   {
      /// <summary>
      /// Returns the list of all table names in the table storage.
      /// </summary>
      /// <returns></returns>
      Task<IEnumerable<string>> ListTableNamesAsync();

      /// <summary>
      /// Deletes entire table. If table doesn't exist no errors are raised.
      /// </summary>
      /// <param name="tableName">Name of the table to delete. Passing null raises <see cref="ArgumentNullException"/></param>
      Task DeleteAsync(string tableName);

      /// <summary>
      /// Gets rows by partition key.
      /// </summary>
      /// <param name="tableName">Table name, required.</param>
      /// <param name="partitionKey">Partition key of the table, required.</param>
      /// <returns>
      /// List of table rows in the table's partition. This method never returns null and if no records
      /// are found an empty collection is returned.
      /// </returns>
      Task<IEnumerable<TableRow>> GetAsync(string tableName, string partitionKey);

      /// <summary>
      /// Gets a single row by partition key and row key as this uniquely idendifies a row.
      /// </summary>
      /// <param name="tableName">Table name, required.</param>
      /// <param name="partitionKey">Partition key of the table, required.</param>
      /// <param name="rowKey">Row key, required.</param>
      /// <returns>
      /// List of table rows in the table's partition. This method never returns null and if no records
      /// are found an empty collection is returned.
      /// </returns>
      Task<TableRow> GetAsync(string tableName, string partitionKey, string rowKey);

      /// <summary>
      /// Inserts rows in the table.
      /// </summary>
      /// <param name="tableName">Table name, required.</param>
      /// <param name="rows">Rows to insert, required. The rows can belong to different partitions.</param>
      /// <exception cref="StorageException">
      /// If the row already exists throws this exception with <see cref="ErrorCode.DuplicateKey"/>.
      /// Note that exception is thrown only for partiton batch. If rows contains more than one partition to insert
      /// some of them may succeed and some may fail.
      /// </exception>
      Task InsertAsync(string tableName, IEnumerable<TableRow> rows);

      /// <summary>
      /// Inserts rows in the table, and if they exist replaces them with a new value.
      /// </summary>
      /// <param name="tableName">Table name, required.</param>
      /// <param name="rows">Rows to insert, required. The rows can belong to different partitions.</param>
      /// <exception cref="StorageException">
      /// If input rows have duplicated keys throws this exception with <see cref="ErrorCode.DuplicateKey"/>
      /// </exception>
      Task InsertOrReplaceAsync(string tableName, IEnumerable<TableRow> rows);

      /// <summary>
      /// Updates multiple rows. Note that all the rows must belong to the same partition.
      /// </summary>
      Task UpdateAsync(string tableName, IEnumerable<TableRow> rows);

      /// <summary>
      /// Merges multiple rows. Note that all rows must belong to the same partition
      /// </summary>
      Task MergeAsync(string tableName, IEnumerable<TableRow> rows);

      /// <summary>
      /// Deletes multiple rows
      /// </summary>
      Task DeleteAsync(string tableName, IEnumerable<Key> rowIds);
   }
}
