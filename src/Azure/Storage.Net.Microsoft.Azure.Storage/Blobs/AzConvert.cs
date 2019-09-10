using System;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.Storage.File;
using NetBox.Extensions;
using Storage.Net.Blobs;

namespace Storage.Net.Microsoft.Azure.Storage.Blobs
{
   static class AzConvert
   {
      public static Blob ToBlob(CloudFileShare share)
      {
         var blob = new Blob(share.Name, BlobItemKind.Folder);
         blob.TryAddProperties(
            "IsSnapshot", share.IsSnapshot.ToString(),
            "ETag", share.Properties.ETag,
            "LastModified", share.Properties.LastModified?.ToString(),
            "Quota", share.Properties.Quota?.ToString(),
            "SnapshotTime", share.SnapshotTime?.ToString(),
            "IsShare", "True");
         blob.Metadata.MergeRange(share.Metadata);
         return blob;
      }

      public static Blob ToBlob(string path, IListFileItem item)
      {
         if(item is CloudFile file)
         {
            var blob = new Blob(path, file.Name, BlobItemKind.File)
            {
               LastModificationTime = file.Properties.LastWriteTime,
               Size = file.Properties.Length,
               MD5 = file.Properties.ContentMD5
            };
            blob.TryAddProperties(
               "CopyState", file.CopyState?.ToString(),
               "ChangeTime", file.Properties.ChangeTime?.ToString(),
               "ContentType", file.Properties.ContentType,
               "CreationTime", file.Properties.CreationTime?.ToString(),
               "ETag", file.Properties.ETag,
               "IsServerEncrypted", file.Properties.IsServerEncrypted.ToString(),
               "LastModified", file.Properties.LastModified?.ToString(),
               "NtfsAttributes", file.Properties.NtfsAttributes?.ToString());
            blob.Metadata.MergeRange(file.Metadata);
            return blob;
         }
         else if(item is CloudFileDirectory dir)
         {
            var blob = new Blob(path, dir.Name, BlobItemKind.Folder)
            {
               LastModificationTime = dir.Properties.LastWriteTime
            };
            blob.TryAddProperties(
               "ChangeTime", dir.Properties.ChangeTime?.ToString(),
               "CreationTime", dir.Properties.CreationTime?.ToString(),
               "ETag", dir.Properties.ETag,
               "IsServerEncrypted", dir.Properties.IsServerEncrypted.ToString(),
               "LastModified", dir.Properties.LastModified?.ToString(),
               "NtfsAttributes", dir.Properties.NtfsAttributes?.ToString());
            blob.Metadata.MergeRange(dir.Metadata);
            return blob;
         }
         else
         {
            throw new NotSupportedException($"don't know '{item.GetType()}' object type");
         }
      }

      public static Blob ToBlob(CloudBlobContainer container)
      {
         var blob = new Blob(container.Name, BlobItemKind.Folder);
         blob.Properties["IsContainer"] = "True";
         
         if(container.Properties != null)
         {
            BlobContainerProperties props = container.Properties;
            if(props.ETag != null)
               blob.Properties["ETag"] = props.ETag;
            if(props.HasImmutabilityPolicy != null)
               blob.Properties["HasImmutabilityPolicy"] = props.HasImmutabilityPolicy.ToString();
            if(props.HasLegalHold != null)
               blob.Properties["HasLegalHold"] = props.HasLegalHold.ToString();
            blob.Properties["LeaseDuration"] = props.LeaseDuration.ToString();
            blob.Properties["LeaseState"] = props.LeaseState.ToString();
            blob.Properties["LeaseStatus"] = props.LeaseStatus.ToString();
            if(props.PublicAccess != null)
               blob.Properties["PublicAccess"] = props.PublicAccess.ToString();
         }

         if(container.Metadata?.Count > 0)
         {
            blob.Metadata.MergeRange(container.Metadata);
         }
         
         return blob;
      }
   }
}
