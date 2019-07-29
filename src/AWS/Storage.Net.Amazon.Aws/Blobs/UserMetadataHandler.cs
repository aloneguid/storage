using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;
using Storage.Net.Blobs;

namespace Storage.Net.Amazon.Aws.Blobs
{
   static class Converter
   {
      /// <summary>
      /// AWS prepends all the user metadata with this prefix, and all of your own keys are prepended with this automatically
      /// </summary>
      private const String MetaDataHeaderPrefix = "x-amz-meta-";

      public static async Task UpdateMetadataAsync(AmazonS3Client client, Blob blob, string bucketName, string key)
      {
         var request = new PutObjectRequest
         {
            BucketName = bucketName,
            Key = key
         };

         foreach(KeyValuePair<string, string> pair in blob.Metadata)
         {
            request.Metadata[pair.Key] = pair.Value;
         }

         await client.PutObjectAsync(request).ConfigureAwait(false);
      }

      public static async Task AppendMetadataAsync(AmazonS3Client client, string bucketName, Blob blob, CancellationToken cancellationToken)
      {
         if(blob == null)
            return;

         GetObjectResponse obj = await client.GetObjectAsync(bucketName, blob.FullPath, cancellationToken).ConfigureAwait(false);

         foreach(string key in obj.Metadata.Keys)
         {
            string value = obj.Metadata[key];
            string putKey = key;
            if(putKey.StartsWith(MetaDataHeaderPrefix))
               putKey = putKey.Substring(MetaDataHeaderPrefix.Length);

            blob.Metadata[putKey] = value;
         }
      }

      public static async Task AppendMetadataAsync(AmazonS3Client client, string bucketName, IEnumerable<Blob> blobs, CancellationToken cancellationToken)
      {
         await Task.WhenAll(blobs.Select(blob => AppendMetadataAsync(client, bucketName, blob, cancellationToken))).ConfigureAwait(false);
      }

   }
}
