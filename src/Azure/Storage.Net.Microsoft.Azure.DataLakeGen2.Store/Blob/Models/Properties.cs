using System;
using System.Net.Http.Headers;

namespace Storage.Net.Microsoft.Azure.DataLakeGen2.Store.Blob.Models
{
   public class Properties
   {
      public MediaTypeHeaderValue ContentType { get; set; }
      public DateTimeOffset LastModified { get; set; }
      public long Length { get; set; }
      public bool IsDirectory { get; set; }
   }
}