using System.Collections.Generic;
using Newtonsoft.Json;

namespace Storage.Net.Microsoft.Azure.DataLakeGen2.Store.Blob.Models
{
   public class DirectoryList
   {
      [JsonProperty("paths")] public List<DirectoryItem> Paths { get; set; }
   }
}