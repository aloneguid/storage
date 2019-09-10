using System;
using System.Collections.Generic;
using System.Text;
using NetBox.Extensions;
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

         blob.TryAddProperties(
            "ETag", path.ETag,
            "Owner", path.Owner,
            "Group", path.Group,
            "Permissions", path.Permissions);

         return blob;
      }

      public static Blob ToBlob(string fullPath, PathProperties path)
      {
         var blob = new Blob(fullPath)
         {
            Size = path.Length,
            LastModificationTime = path.LastModified
         };

         blob.TryAddProperties(
            "ETag", path.ETag,
            "ContentType", path.ContentType,
            "ResourceType", path.ResourceType);

         if(path.UserMetadata != null)
            blob.Metadata.MergeRange(path.UserMetadata);

         return blob;
      }
   }
}
