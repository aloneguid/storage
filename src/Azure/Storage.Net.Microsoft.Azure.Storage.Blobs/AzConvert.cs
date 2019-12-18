using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using NetBox.Extensions;
using Storage.Net.Blobs;
using Storage.Net.Microsoft.Azure.Storage.Blobs.Gen2.Model;

namespace Storage.Net.Microsoft.Azure.Storage.Blobs
{
   static class AzConvert
   {
      private static readonly char[] MetadataPairsSeparator = new[] { ',' };
      private static readonly char[] MetadataPairSeparator = new[] { '=' };

      public static Blob ToBlob(Filesystem fs)
      {
         var blob = new Blob(fs.Name, BlobItemKind.Folder);
         blob.LastModificationTime = fs.LastModified;
         blob.TryAddProperties(
            "ETag", fs.Etag,
            "IsFilesystem", true);
         return blob;
      }

      public static Blob ToBlob(BlobContainerItem item)
      {
         var blob = new Blob(item.Name, BlobItemKind.Folder);
         blob.TryAddProperties(
            "IsContainer", true);

         return blob;
      }

      public static Blob ToBlob(BlobContainerClient client)
      {
         var blob = new Blob(client.Name, BlobItemKind.Folder);
         blob.Properties["IsContainer"] = true;
         if(client.Name == "$logs")
         {
            blob.Properties["IsLogsContainer"] = true;
         }
         return blob;
      }

      public static Blob ToBlob(string name, Response<BlobContainerProperties> properties)
      {
         return ToBlob(name, properties.Value);
      }

      public static Blob ToBlob(string name, BlobContainerProperties properties)
      {
         var blob = new Blob(name, BlobItemKind.Folder);
         blob.LastModificationTime = properties.LastModified;

         blob.TryAddProperties(
            "IsContainer", true,
            "ETag", properties.ETag,
            "HasImmutabilityPolicy", properties.HasImmutabilityPolicy,
            "HasLegalHold", properties.HasLegalHold,
            "LastModified", properties.LastModified,
            "LeaseDuration", properties.LeaseDuration,
            "LeaseState", properties.LeaseState,
            "LeaseStatus", properties.LeaseStatus,
            "PublicAccess", properties.PublicAccess);

         blob.Metadata.MergeRange(properties.Metadata);

         return blob;
      }

      public static Blob ToBlob(string containerName, BlobHierarchyItem bhi)
      {
         string GetFullName(string name) => containerName == null
            ? name
            : StoragePath.Combine(containerName, name);

         if(bhi.IsBlob)
         {
            var blob = new Blob(GetFullName(bhi.Blob.Name), BlobItemKind.File);
            blob.MD5 = bhi.Blob.Properties.ContentHash.ToHexString();
            blob.Size = bhi.Blob.Properties.ContentLength;
            blob.LastModificationTime = bhi.Blob.Properties.LastModified;

            AddProperties(blob, bhi.Blob.Properties);
            blob.Metadata.MergeRange(bhi.Blob.Metadata);

            return blob;
         }

         if(bhi.IsPrefix)
         {
            var blob = new Blob(GetFullName(bhi.Prefix), BlobItemKind.Folder);
            //nothing else we know about prefix
            return blob;
         }

         throw new NotImplementedException();
      }

      public static Blob ToBlob(string containerName, string path, Response<BlobProperties> properties)
      {
         return ToBlob(containerName, path, properties.Value);
      }

      public static Blob ToBlob(string containerName, string path, BlobProperties properties)
      {
         string GetFullName(string name) => containerName == null
            ? name
            : StoragePath.Combine(containerName, name);

         var blob = new Blob(GetFullName(path), BlobItemKind.File);
         blob.MD5 = properties.ContentHash.ToHexString();
         blob.Size = properties.ContentLength;
         blob.LastModificationTime = properties.LastModified;

         AddProperties(blob, properties);

         blob.Metadata.MergeRange(properties.Metadata);

         return blob;
      }

      private static void AddProperties(Blob blob, BlobItemProperties properties)
      {
         blob.TryAddProperties(
            "CustomerProvidedKeySha256", properties.CustomerProvidedKeySha256,
            "IncrementalCopy", properties.IncrementalCopy,
            "ServerEncrypted", properties.ServerEncrypted,
            "DeletedOn", properties.DeletedOn,
            "RemainingRetentionDays", properties.RemainingRetentionDays,
            "AccessTier", properties.AccessTier,
            "AccessTierChangedOn", properties.AccessTierChangedOn,
            "AccessTierInferred", properties.AccessTierInferred,
            "ArchiveStatus", properties.ArchiveStatus,
            "BlobSequenceNumber", properties.BlobSequenceNumber,
            "BlobType", properties.BlobType,
            "CacheControl", properties.CacheControl,
            "ContentDisposition", properties.ContentDisposition,
            "ContentEncoding", properties.ContentEncoding,
            "ContentHash", properties.ContentHash.ToHexString(),
            "ContentLanguage", properties.ContentLanguage,
            "ContentLength", properties.ContentLength,
            "ContentType", properties.ContentType,
            "CopyCompletedOn", properties.CopyCompletedOn,
            "CopyId", properties.CopyId,
            "CopyProgress", properties.CopyProgress,
            "CopySource", properties.CopySource,
            "CopyStatus", properties.CopyStatus,
            "CopyStatusDescription", properties.CopyStatusDescription,
            "CreatedOn", properties.CreatedOn,
            "DestinationSnapshot", properties.DestinationSnapshot,
            "ETag", properties.ETag,
            "LastModified", properties.LastModified,
            "LeaseDuration", properties.LeaseDuration,
            "LeaseState", properties.LeaseState,
            "LeaseStatus", properties.LeaseStatus);
      }

      private static void AddProperties(Blob blob, BlobProperties properties)
      {
         blob.TryAddProperties(
            "AcceptRanges", properties.AcceptRanges,
            "AccessTier", properties.AccessTier,
            "AccessTierChangedOn", properties.AccessTierChangedOn,
            "AccessTierInferred", properties.AccessTierInferred,
            "ArchiveStatus", properties.ArchiveStatus,
            "BlobCommittedBlockCount", properties.BlobCommittedBlockCount,
            "BlobSequenceNumber", properties.BlobSequenceNumber,
            "BlobType", properties.BlobType,
            "CacheControl", properties.CacheControl,
            "ContentDisposition", properties.ContentDisposition,
            "ContentEncoding", properties.ContentEncoding,
            "ContentHash", properties.ContentHash,
            "ContentLanguage", properties.ContentLanguage,
            "ContentLength", properties.ContentLength,
            "ContentType", properties.ContentType,
            "CopyCompletedOn", properties.CopyCompletedOn,
            "CopyId", properties.CopyId,
            "CopyProgress", properties.CopyProgress,
            "CopySource", properties.CopySource,
            "CopyStatus", properties.CopyStatus,
            "CopyStatusDescription", properties.CopyStatusDescription,
            "CreatedOn", properties.CreatedOn,
            "DestinationSnapshot", properties.DestinationSnapshot,
            "EncryptionKeySha256", properties.EncryptionKeySha256,
            "ETag", properties.ETag,
            "IsIncrementalCopy", properties.IsIncrementalCopy,
            "IsServerEncrypted", properties.IsServerEncrypted,
            "LastModified", properties.LastModified,
            "LeaseDuration", properties.LeaseDuration,
            "LeaseState", properties.LeaseState,
            "LeaseStatus", properties.LeaseStatus);
      }

      public static Blob ToBlob(string filesystemName, Gen2Path path)
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

      public static Blob ToBlob(string fullPath, IDictionary<string, string> pathHeaders, bool isFilesystem)
      {
         var blob = new Blob(fullPath);

         if(pathHeaders.TryGetValue("Content-MD5", out string md5))
         {
            blob.MD5 = md5.Base64DecodeAsBytes().ToHexString();
         }

         if(pathHeaders.TryGetValue("Content-Length", out string contentLength))
         {
            blob.Size = long.Parse(contentLength);
         }

         blob.LastModificationTime = DateTimeOffset.Parse(pathHeaders["Last-Modified"]);

         blob.TryAddPropertiesFromDictionary(pathHeaders,
            "Content-Type",
            "ETag",
            "x-ms-owner",
            "x-ms-group",
            "x-ms-permissions",
            "x-ms-resource-type",
            "x-ms-lease-state",
            "x-ms-lease-status",
            "x-ms-server-encrypted",
            "x-ms-request-id",
            "x-ms-client-request-id");

         if(isFilesystem)
         {
            blob.Properties["IsFilesystem"] = isFilesystem;
         }

         if(pathHeaders.TryGetValue("x-ms-properties", out string um))
         {
            Dictionary<string, string> umd = um
               .Split(MetadataPairsSeparator, StringSplitOptions.RemoveEmptyEntries)
               .Select(pair => pair.Split(MetadataPairSeparator, 2))
               .ToDictionary(a => a[0], a => a[1].Base64Decode());

            blob.Metadata.MergeRange(umd);
         }

         return blob;
      }
   }
}
