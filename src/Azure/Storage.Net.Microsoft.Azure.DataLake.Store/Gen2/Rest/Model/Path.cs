using System;
using Newtonsoft.Json;

namespace Storage.Net.Microsoft.Azure.DataLake.Store.Gen2.Rest.Model
{
   class Path
   {
      [JsonProperty("contentLength")]
      public int ContentLength { get; set; }

      [JsonProperty("eTag")]
      public string ETag { get; set; }

      [JsonProperty("group")]
      public string Group { get; set; }

      [JsonProperty("isDirectory")]
      public bool IsDirectory { get; set; }

      [JsonProperty("lastModified")]
      public DateTimeOffset LastModified { get; set; }

      [JsonProperty("name")]
      public string Name { get; set; }

      [JsonProperty("owner")]
      public string Owner { get; set; }

      [JsonProperty("permissions")]
      public string Permissions { get; set; }

      public override string ToString() => $"{(IsDirectory? "dir" : "file")} {Name}";
   }
}
