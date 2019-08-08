using Newtonsoft.Json;

namespace Storage.Net.Microsoft.Azure.DataLake.Store.Gen2.Rest.Model
{
   class PathList
   {
      [JsonProperty("paths")]
      public Path[] Paths { get; set; }

      public override string ToString() => $"{Paths?.Length ?? 0}";
   }
}
