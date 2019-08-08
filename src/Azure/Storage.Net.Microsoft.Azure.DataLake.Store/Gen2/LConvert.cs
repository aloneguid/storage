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

      public static Blob ToBlob(string filesystemName, Path path)
      {
         var blob = new Blob(StoragePath.Combine(filesystemName, path.Name), path.IsDirectory ? BlobItemKind.Folder : BlobItemKind.File)
         {
            Size = path.ContentLength,
            LastModificationTime = path.LastModified
         };
         blob.Properties["ETag"] = path.ETag;
         blob.Properties["Owner"] = path.Owner;
         blob.Properties["Group"] = path.Group;
         blob.Properties["Permissions"] = path.Permissions;

         return blob;
      }

      public static Blob ToBlob(string fullPath, PathProperties pp)
      {
         var result = new Blob(fullPath)
         {
            Size = pp.Length,
            LastModificationTime = pp.LastModified
         };

         return result;
      }
   }
}
