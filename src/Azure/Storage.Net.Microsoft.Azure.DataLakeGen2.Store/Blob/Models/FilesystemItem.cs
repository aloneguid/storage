using System;
using Newtonsoft.Json;

namespace Storage.Net.Microsoft.Azure.DataLakeGen2.Store.Blob.Models
{
   public class FilesystemItem
   {
      [JsonProperty("etag")]
      public string Etag { get; set; }

      [JsonProperty("lastModified")]
      public DateTime LastModified { get; set; }

      [JsonProperty("name")]
      public string Name { get; set; }
   }
}
