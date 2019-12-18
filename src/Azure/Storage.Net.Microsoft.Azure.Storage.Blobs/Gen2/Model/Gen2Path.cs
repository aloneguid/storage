using System;
using System.Text.Json.Serialization;

namespace Storage.Net.Microsoft.Azure.Storage.Blobs.Gen2.Model
{
   class Gen2Path
   {
      [JsonPropertyName("contentLength")]
      public string ContentLengthJson
      {
         get => ContentLength.ToString();
         set
         {
            ContentLength = long.Parse(value);
         }
      }

      [JsonIgnore]   //this arrives as a string for some odd reason
      public long ContentLength { get; set; }

      public string ETag { get; set; }

      public string Group { get; set; }

      [JsonPropertyName("isDirectory")]
      public string IsDirectoryJson
      {
         get => IsDirectory.ToString();
         set => IsDirectory = bool.Parse(value);
      }

      [JsonIgnore]   //for some reason it's also passed as a string
      public bool IsDirectory { get; set; }

      public DateTime LastModified { get; set; }

      public string Name { get; set; }

      public string Owner { get; set; }

      public string Permissions { get; set; }

      public override string ToString() => $"{(IsDirectory ? "dir" : "file")} {Name}";
   }
}
