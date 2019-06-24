using System.Collections.Generic;
using Newtonsoft.Json;

namespace Storage.Net.Microsoft.Azure.DataLakeGen2.Store.Blob.Models
{
   public class FilesystemList
   {
      [JsonProperty("filesystems")]
      public List<FilesystemItem> Filesystems { get; set; }
   }
}
