using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Storage.Net.KeyValue
{
   /// <summary>
   /// <see cref="IKeyValueStorage"/> on steroids
   /// </summary>
   public static class KeyValueStorageExtensions
   {
      /// <summary>
      /// Gets first value by key
      /// </summary>
      /// <param name="storage"></param>
      /// <param name="tableName"></param>
      /// <param name="key"></param>
      /// <returns></returns>
      public static async Task<Value> GetSingleAsync(this IKeyValueStorage storage, string tableName, Key key)
      {
         IReadOnlyCollection<Value> values = await storage.GetAsync(tableName, key);

         return values.FirstOrDefault();
      }

      /// <summary>
      /// Gets first value by key
      /// </summary>
      /// <param name="storage"></param>
      /// <param name="tableName"></param>
      /// <param name="partitionKey"></param>
      /// <param name="rowKey"></param>
      /// <returns></returns>
      public static async Task<Value> GetSingleAsync(this IKeyValueStorage storage, string tableName, string partitionKey, string rowKey)
      {
         if (rowKey == null) throw new ArgumentNullException(nameof(rowKey));

         IReadOnlyCollection<Value> values = await storage.GetAsync(tableName, new Key(partitionKey, rowKey));

         return values.FirstOrDefault();
      }

      /// <summary>
      /// Deletes record by key
      /// </summary>
      public static Task DeleteAsync(this IKeyValueStorage storage, string tableName, Key key)
      {
         return storage.DeleteAsync(tableName, new[] { key });
      }
   }
}