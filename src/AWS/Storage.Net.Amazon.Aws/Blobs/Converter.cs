using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;
using NetBox.Extensions;
using Storage.Net.Blobs;

namespace Storage.Net.Amazon.Aws.Blobs
{
   static class Converter
   {
      /// <summary>
      /// AWS prepends all the user metadata with this prefix, and all of your own keys are prepended with this automatically
      /// </summary>
      private const string MetaDataHeaderPrefix = "x-amz-meta-";

      public static async Task UpdateMetadataAsync(AmazonS3Client client, Blob blob, string bucketName, string key)
      {
         // there is no way to update metadata in S3, and the only way is to recreate it
         // however, you can copy object on top of itself (effectively a replace) and rewrite metadata, and this won't have to download the blob on the client

         var request = new CopyObjectRequest
         {
            SourceBucket = bucketName,
            DestinationBucket = bucketName,
            SourceKey = key,
            DestinationKey = key,
            MetadataDirective = S3MetadataDirective.REPLACE
         };


         foreach(KeyValuePair<string, string> pair in blob.Metadata)
         {
            request.Metadata[pair.Key] = pair.Value;
         }

         await client.CopyObjectAsync(request);
      }

      private static async Task AppendMetadataAsync(AmazonS3Client client, string bucketName, Blob blob, CancellationToken cancellationToken)
      {
         if(blob == null)
            return;

         GetObjectMetadataResponse obj = await client.GetObjectMetadataAsync(bucketName, blob.FullPath.Substring(1), cancellationToken).ConfigureAwait(false);

         AddMetadata(blob, obj.Metadata);
      }

      public static async Task AppendMetadataAsync(AmazonS3Client client, string bucketName, IEnumerable<Blob> blobs, CancellationToken cancellationToken)
      {
         await Task.WhenAll(
            blobs.Select(blob => AppendMetadataAsync(client, bucketName, blob, cancellationToken))).ConfigureAwait(false);
      }

      public static Blob ToBlob(this GetObjectMetadataResponse obj, string fullPath)
      {
         if(obj == null)
            return null;

         var r = new Blob(fullPath);
         r.MD5 = obj.ETag.Trim('\"'); //ETag contains actual MD5 hash, not sure why!
         r.Size = obj.ContentLength;
         r.LastModificationTime = obj.LastModified.ToUniversalTime();

         AddMetadata(r, obj.Metadata);

         r.Properties["ETag"] = obj.ETag;

         return r;
      }

      private static void AddMetadata(Blob blob, MetadataCollection metadata)
      {
         //add metadata and strip all
         foreach(string key in metadata.Keys)
         {
            string value = metadata[key];
            string putKey = key;
            if(putKey.StartsWith(MetaDataHeaderPrefix))
               putKey = putKey.Substring(MetaDataHeaderPrefix.Length);

            blob.Metadata[putKey] = value;
         }
      }

      public static Blob ToBlob(this S3Object s3Obj)
      {
         Blob blob = s3Obj.Key.EndsWith("/")
            ? new Blob(s3Obj.Key, BlobItemKind.Folder)
            //Key is an absolute path
            : new Blob(s3Obj.Key, BlobItemKind.File);

         blob.Size = s3Obj.Size;
         blob.MD5 = s3Obj.ETag.Trim('\"');
         blob.LastModificationTime = s3Obj.LastModified.ToUniversalTime();
         blob.Properties["StorageClass"] = s3Obj.StorageClass;
         blob.Properties["ETag"] = s3Obj.ETag;

         return blob;
      }

      public static IReadOnlyCollection<Blob> ToBlobs(this ListObjectsV2Response response, ListOptions options)
      {
         var result = new List<Blob>();

         //the files are listed as the S3Objects member, but they don't specifically contain folders,
         //but even if they do, they need to be filtered out

         result.AddRange(
            response.S3Objects
               .Where(b => !b.Key.EndsWith("/")) //check if this is "virtual folder" as S3 console creates them (rubbish)
               .Select(b => b.ToBlob())
               .Where(options.IsMatch)
               .Where(b => options.BrowseFilter == null || options.BrowseFilter(b)));

         //subfolders are listed in another field (what a funny name!)

         //prefix is absolute too
         result.AddRange(
            response.CommonPrefixes
               .Where(p => !StoragePath.IsRootPath(p))
               .Select(p => new Blob(p, BlobItemKind.Folder)));

         return result;
      }


   }
}
