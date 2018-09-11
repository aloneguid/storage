using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Storage.Net.KeyValue
{
   /// <summary>
   /// Represents a table row in table data structure.
   /// </summary>
   public class Value : IDictionary<string, object>, IEquatable<Value>
   {
      private readonly Dictionary<string, object> _keyToValue = new Dictionary<string, object>(); 

      /// <summary>
      /// Creates a new instance from partition key and row key
      /// </summary>
      public Value(string partitionKey, string rowKey) : this(new Key(partitionKey, rowKey))
      {
      }

      /// <summary>
      /// Creates a new instance from <see cref="Key"/>
      /// </summary>
      /// <param name="id"></param>
      public Value(Key id)
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
      public bool Equals(Value other)
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
      public IEnumerator<KeyValuePair<string, object>> GetEnumerator()
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
      public void Add(KeyValuePair<string, object> item)
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
      public bool Contains(KeyValuePair<string, object> item)
      {
         return _keyToValue.ContainsKey(item.Key);
      }

      /// <summary>
      /// IDictionary.CopyTo
      /// </summary>
      public void CopyTo(KeyValuePair<string, object>[] array, int arrayIndex)
      {
         throw new NotSupportedException();
      }

      /// <summary>
      /// IDictionary.Remove
      /// </summary>
      public bool Remove(KeyValuePair<string, object> item)
      {
         _keyToValue.Remove(item.Key);

         return true;
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
      public void Add(string key, object value)
      {
         if(value == null)
         {
            Remove(key);
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
         return _keyToValue.Remove(key);
      }

      /// <summary>
      /// IDictionary.TryGetValue
      /// </summary>
      public bool TryGetValue(string key, out object value)
      {
         return _keyToValue.TryGetValue(key, out value);
      }

      /// <summary>
      /// IDictionary.this
      /// </summary>
      public object this[string key]
      {
         get
         {
            if (!_keyToValue.TryGetValue(key, out object value)) return null;
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
      public ICollection<object> Values
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
      public Value Clone(string rowKey = null, string partitionKey = null)
      {
         var clone = new Value(partitionKey ?? PartitionKey, rowKey ?? RowKey);
         foreach(KeyValuePair<string, object> pair in _keyToValue)
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
      public static bool AreDistinct(IEnumerable<Value> rows)
      {
         if (rows == null) return true;

         IEnumerable<IGrouping<Key, Value>> groups = rows.GroupBy(r => r.Id);
         IEnumerable<int> counts = groups.Select(g => g.Count());
         return counts.OrderByDescending(c => c).First() == 1;
      }

      public static Value Merge(IEnumerable<Value> rows)
      {
         Value masterRow = null;

         foreach (Value row in rows)
         {
            if (masterRow == null)
            {
               masterRow = row;
            }
            else
            {
               foreach (KeyValuePair<string, object> cell in row)
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
