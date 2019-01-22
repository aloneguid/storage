using System;
using System.Collections.Generic;

namespace Storage.Net.Blob
{
   /// <summary>
   /// Contains basic metadata about a blob
   /// </summary>
   public class BlobMeta
   {
      /// <summary>
      /// Creates an instance of blob metadata
      /// </summary>
      /// <param name="size">Blob size</param>
      /// <param name="md5">MD5 hash of the blob</param>
      /// <param name="lastModificationTime">Last modifiacation time when known</param>
      public BlobMeta(long size, string md5, DateTimeOffset? lastModificationTime)
      {
         this.Size = size;
         this.MD5 = md5;
         LastModificationTime = lastModificationTime;
      }

      /// <summary>
      /// Blob size
      /// </summary>
      public long Size { get; }

      /// <summary>
      /// MD5 content hash of the blob. Note that this property can be null if underlying storage has
      /// no information about the hash.
      /// </summary>
      public string MD5 { get; }

      /// <summary>
      /// Last modification time when known
      /// </summary>
      public DateTimeOffset? LastModificationTime { get; }

      /// <summary>
      /// Extra properties that are implementation specific and have no specification. Take an extra care when using these are they are not
      /// guaranteed to be present at all or change between implementaitons.
      /// </summary>
      public Dictionary<string, object> Properties { get; } = new Dictionary<string, object>();
   }
}
