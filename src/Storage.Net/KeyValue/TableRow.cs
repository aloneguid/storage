using NetBox;
using NetBox.Data;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace Storage.Net.KeyValue
{
   /// <summary>
   /// Represents a table row in table data structure.
   /// </summary>
   public class TableRow : IDictionary<string, DynamicValue>, IEquatable<TableRow>
   {
      private readonly ConcurrentDictionary<string, DynamicValue> _keyToValue = new ConcurrentDictionary<string, DynamicValue>(); 

      /// <summary>
      /// Creates a new instance from partition key and row key
      /// </summary>
      public TableRow(string partitionKey, string rowKey) : this(new Key(partitionKey, rowKey))
      {
      }

      /// <summary>
      /// Creates a new instance from <see cref="Key"/>
      /// </summary>
      /// <param name="id"></param>
      public TableRow(Key id)
      {
         Id = id ?? throw new ArgumentNullException(nameof(id));
      }

      /// <summary>
      /// Row ID
      /// </summary>
      public Key Id { get; private set; }

      /// <summary>
      /// Partition key
      /// </summary>
      public string PartitionKey { get { return Id.PartitionKey; }}

      /// <summary>
      /// Row key
      /// </summary>
      public string RowKey { get { return Id.RowKey; }}

      /// <summary>
      /// Checks row equality
      /// </summary>
      public bool Equals(TableRow other)
      {
         if(ReferenceEquals(other, null)) return false;
         if(ReferenceEquals(other, this)) return true;
         if(GetType() != other.GetType()) return false;

         return other.Id.PartitionKey == Id.PartitionKey && other.Id.RowKey == Id.RowKey;
      }

      #region [IDictionary]

      /// <summary>
      /// Get enumerator for cells inside the row
      /// </summary>
      public IEnumerator<KeyValuePair<string, DynamicValue>> GetEnumerator()
      {
         return _keyToValue.GetEnumerator();
      }

      /// <summary>
      /// Get enumerator for cells inside the row
      /// </summary>
      IEnumerator IEnumerable.GetEnumerator()
      {
         return GetEnumerator();
      }

      /// <summary>
      /// IDictionary.Add
      /// </summary>
      public void Add(KeyValuePair<string, DynamicValue> item)
      {
         Add(item.Key, item.Value);
      }

      /// <summary>
      /// Clears cells
      /// </summary>
      public void Clear()
      {
         _keyToValue.Clear();
      }

      /// <summary>
      /// IDictionary.Contains
      /// </summary>
      public bool Contains(KeyValuePair<string, DynamicValue> item)
      {
         return _keyToValue.ContainsKey(item.Key);
      }

      /// <summary>
      /// IDictionary.CopyTo
      /// </summary>
      public void CopyTo(KeyValuePair<string, DynamicValue>[] array, int arrayIndex)
      {
         throw new NotSupportedException();
      }

      /// <summary>
      /// IDictionary.Remove
      /// </summary>
      public bool Remove(KeyValuePair<string, DynamicValue> item)
      {
         return _keyToValue.TryRemove(item.Key, out DynamicValue value);
      }

      /// <summary>
      /// IDictionary.Count
      /// </summary>
      public int Count
      {
         get { return _keyToValue.Count; }
      }

      /// <summary>
      /// IDictionary.IsReadOnly
      /// </summary>
      public bool IsReadOnly
      {
         get { return false; }
      }

      /// <summary>
      /// IDictionary.Add
      /// </summary>
      public void Add(string key, DynamicValue value)
      {
         if(value == null)
         {
            _keyToValue.TryRemove(key, out value);
         }
         else
         {
            _keyToValue[key] = value;
         }
      }

      /// <summary>
      /// IDictionary.ContainsKey
      /// </summary>
      public bool ContainsKey(string key)
      {
         return _keyToValue.ContainsKey(key);
      }

      /// <summary>
      /// IDictionary.Remove
      /// </summary>
      public bool Remove(string key)
      {
         return _keyToValue.TryRemove(key, out DynamicValue value);
      }

      /// <summary>
      /// IDictionary.TryGetValue
      /// </summary>
      public bool TryGetValue(string key, out DynamicValue value)
      {
         return _keyToValue.TryGetValue(key, out value);
      }

      /// <summary>
      /// IDictionary.this
      /// </summary>
      public DynamicValue this[string key]
      {
         get
         {
            if (!_keyToValue.TryGetValue(key, out DynamicValue value)) return null;
            return value;
         }
         set { Add(key, value); }
      }

      /// <summary>
      /// IDictionary.Keys
      /// </summary>
      public ICollection<string> Keys
      {
         get { return _keyToValue.Keys; }
      }

      /// <summary>
      /// IDictionary.Values
      /// </summary>
      public ICollection<DynamicValue> Values
      {
         get { return _keyToValue.Values; }
      }

      #endregion

      /// <summary>
      /// Clones the row
      /// </summary>
      /// <param name="rowKey">When specified, the clone receives this value for the Row Key</param>
      /// <param name="partitionKey">When speified, the clone receives this value for the Partition Key</param>
      /// <returns></returns>
      public TableRow Clone(string rowKey = null, string partitionKey = null)
      {
         var clone = new TableRow(partitionKey ?? PartitionKey, rowKey ?? RowKey);
         foreach(KeyValuePair<string, DynamicValue> pair in _keyToValue)
         {
            clone._keyToValue[pair.Key] = pair.Value;
         }
         return clone;
      }

      /// <summary>
      /// Returns string representation
      /// </summary>
      /// <returns></returns>
      public override string ToString()
      {
         return $"{PartitionKey} : {RowKey}";
      }

      /// <summary>
      /// Checks if all rows have uniqueue keys
      /// </summary>
      public static bool AreDistinct(IEnumerable<TableRow> rows)
      {
         if (rows == null) return true;

         IEnumerable<IGrouping<Key, TableRow>> groups = rows.GroupBy(r => r.Id);
         IEnumerable<int> counts = groups.Select(g => g.Count());
         return counts.OrderByDescending(c => c).First() == 1;
      }

      public static TableRow Merge(IEnumerable<TableRow> rows)
      {
         TableRow masterRow = null;

         foreach (TableRow row in rows)
         {
            if (masterRow == null)
            {
               masterRow = row;
            }
            else
            {
               foreach (KeyValuePair<string, DynamicValue> cell in row)
               {
                  if (!masterRow.ContainsKey(cell.Key))
                  {
                     masterRow[cell.Key] = cell.Value;
                  }
               }
            }
         }

         return masterRow;
      }
   }
}
