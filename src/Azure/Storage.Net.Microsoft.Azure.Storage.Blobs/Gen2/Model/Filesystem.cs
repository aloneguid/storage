using System;

namespace Storage.Net.Microsoft.Azure.Storage.Blobs.Gen2.Model
{

   /// <summary>
   /// Gen 2 filesystem
   /// </summary>
   public class Filesystem
   {
      /// <summary>
      /// ETag
      /// </summary>
      public string Etag { get; set; }

      /// <summary>
      /// Last modification date
      /// </summary>
      public DateTime LastModified { get; set; }

      /// <summary>
      /// Name of the filesystem
      /// </summary>
      public string Name { get; set; }
   }
}
