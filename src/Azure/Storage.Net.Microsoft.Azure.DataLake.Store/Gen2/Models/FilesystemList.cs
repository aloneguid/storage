using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using Storage.Net.Microsoft.Azure.DataLake.Store.Gen2.Rest.Model;

namespace Storage.Net.Microsoft.Azure.DataLake.Store.Gen2.Models
{
   class FilesystemList
   {
      [JsonProperty("filesystems")]
      public List<FilesystemItem> Filesystems { get; set; }

      public static implicit operator FilesystemList(Rest.Model.FilesystemList v) => throw new NotImplementedException();
   }
}
