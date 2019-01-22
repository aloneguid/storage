using System;
using System.Collections.Generic;
using System.Text;

namespace Storage.Net.Microsoft.ServiceFabric.Blob
{
   /// <summary>
   /// Metadata tag
   /// </summary>
   public class BlobMetaTag
   {
      public long Length { get; set; }

      public string Md { get; set; }

      public DateTimeOffset LastModificationTime { get; set; }
   }
}
