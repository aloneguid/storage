using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Storage.Net.KeyValue
{
   /// <summary>
   /// Common interface for working with key-value storage
   /// </summary>
   public interface IKeyValueStorage : IDisposable
   {
      /// <summary>
      /// Returns the list of all table names in the storage.
      /// </summary>
      /// <returns></returns>
      Task<IReadOnlyCollection<string>> ListTableNamesAsync();

      /// <summary>
      /// Deletes entire table. If table doesn't exist no errors are raised.
      /// </summary>
      /// <param name="tableName">Name of the table to delete. Passing null raises <see cref="ArgumentNullException"/></param>
      Task DeleteAsync(string tableName);

      /// <summary>
      /// Gets values by key
      /// </summary>
      /// <param name="tableName">Table name, required.</param>
      /// <param name="key">Row key to look up against. The key must have partition key populated, however row key is optional.
      /// When row key is not set, this method returns all of the values in a specifi</param>
      /// <returns>
      /// List of table values in the table's partition. This method never returns null and if no records
      /// are found an empty collection is returned.
      /// </returns>
      Task<IReadOnlyCollection<Value>> GetAsync(string tableName, Key key);

      /// <summary>
      /// Inserts values in the table.
      /// </summary>
      /// <param name="tableName">Table name, required.</param>
      /// <param name="values">values to insert, required. The values can belong to different partitions.</param>
      /// <exception cref="StorageException">
      /// If the row already exists thvalues this exception with <see cref="ErrorCode.DuplicateKey"/>.
      /// Note that exception is thrown only for partiton batch. If values contains more than one partition to insert
      /// some of them may succeed and some may fail.
      /// </exception>
      Task InsertAsync(string tableName, IReadOnlyCollection<Value> values);

      /// <summary>
      /// Inserts values in the table, and if they exist replaces them with a new value.
      /// </summary>
      /// <param name="tableName">Table name, required.</param>
      /// <param name="values">values to insert, required. The values can belong to different partitions.</param>
      /// <exception cref="StorageException">
      /// If input values have duplicated keys thvalues this exception with <see cref="ErrorCode.DuplicateKey"/>
      /// </exception>
      Task InsertOrReplaceAsync(string tableName, IReadOnlyCollection<Value> values);

      /// <summary>
      /// Updates multiple values. Note that all the values must belong to the same partition.
      /// </summary>
      Task UpdateAsync(string tableName, IReadOnlyCollection<Value> values);

      /// <summary>
      /// Merges multiple values. Note that all values must belong to the same partition
      /// </summary>
      Task MergeAsync(string tableName, IReadOnlyCollection<Value> values);

      /// <summary>
      /// Deletes multiple values
      /// </summary>
      Task DeleteAsync(string tableName, IReadOnlyCollection<Key> rowIds);
   }
}
