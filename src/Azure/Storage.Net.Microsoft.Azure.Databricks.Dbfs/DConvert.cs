using System;
using System.Collections.Generic;
using System.Text;
using Storage.Net.Blobs;
using FileInfo = Microsoft.Azure.Databricks.Client.FileInfo;

namespace Storage.Net.Microsoft.Azure.Databricks.Dbfs
{
   static class DConvert
   {
      public static Blob ToBlob(FileInfo fi)
      {
         return new Blob(
            fi.Path,
            fi.IsDirectory ? BlobItemKind.Folder : BlobItemKind.File)
         {
            Size = fi.IsDirectory ? null : (long?)fi.FileSize
         };
      }
   }
}
