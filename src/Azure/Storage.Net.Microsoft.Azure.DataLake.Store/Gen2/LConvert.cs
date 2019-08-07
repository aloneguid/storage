using System;
using System.Collections.Generic;
using System.Text;
using Storage.Net.Blobs;
using Storage.Net.Microsoft.Azure.DataLake.Store.Gen2.Rest.Model;

namespace Storage.Net.Microsoft.Azure.DataLake.Store.Gen2
{
   static class LConvert
   {
      public static Blob ToBlob(FilesystemItem fs)
      {
         var blob = new Blob(fs.Name, BlobItemKind.Folder) { LastModificationTime = fs.LastModified };
         blob.Properties["IsFilesystem"] = "True";
         return blob;
      }
   }
}
