using System;

namespace Storage.Net.KeyValue
{
   /// <summary>
   /// ID structure of the <see cref="Value"/>
   /// </summary>
   public sealed class Key : IEquatable<Key>
   {
      /// <summary>
      /// Constructs an instance of <see cref="Key"/>
      /// </summary>
      /// <param name="partitionKey">Partition key</param>
      /// <param name="rowKey">Row key</param>
      public Key(string partitionKey, string rowKey) : this(partitionKey, rowKey, "*")
      {
      }

      /// <summary>
      /// Constructs an instance of <see cref="Key"/>
      /// </summary>
      /// <param name="partitionKey">Partition key</param>
      /// <param name="rowKey">Row key</param>
      /// <param name="etag">ETag</param>
      public Key(string partitionKey, string rowKey, string etag)
      {
         if(partitionKey == null)
            throw new ArgumentNullException(nameof(partitionKey));

         PartitionKey = partitionKey;
         RowKey = rowKey;
         ETag = etag;
      }

      /// <summary>
      /// Partition key
      /// </summary>
      public string PartitionKey { get; private set; }

      /// <summary>
      /// Row key
      /// </summary>
      public string RowKey { get; private set; }

      /// <summary>
      /// ETag
      /// </summary>
      public string ETag { get; set; }

      /// <summary>
      /// Equals
      /// </summary>
      public bool Equals(Key other)
      {
         if(ReferenceEquals(other, null))
            return false;
         if(ReferenceEquals(other, this))
            return true;
         if(other.GetType() != GetType())
            return false;

         return other.PartitionKey == PartitionKey && other.RowKey == RowKey && other.ETag == ETag;
      }

      /// <summary>
      /// Equals
      /// </summary>
      public override bool Equals(object obj)
      {
         return Equals(obj as Key);
      }

      /// <summary>
      /// Hash code
      /// </summary>
      public override int GetHashCode()
      {
         return PartitionKey.GetHashCode() * RowKey.GetHashCode();
      }
   }
}
