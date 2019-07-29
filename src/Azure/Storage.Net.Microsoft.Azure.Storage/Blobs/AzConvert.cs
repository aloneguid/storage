using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Azure.Storage.Blob;
using Storage.Net.Blobs;

namespace Storage.Net.Microsoft.Azure.Storage.Blobs
{
   static class AzConvert
   {
      public static Blob ToBlob(CloudBlobContainer container)
      {
         var blob = new Blob(container.Name, BlobItemKind.Folder);
         blob.Properties["IsContainer"] = "True";
         return blob;
      }
   }
}
