using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;
using NetBox.Async;
using Storage.Net.Blobs;

namespace Storage.Net.Amazon.Aws.Blobs
{
   class AwsS3DirectoryBrowser : IDisposable
   {
      private readonly AmazonS3Client _client;
      private readonly string _bucketName;
      private readonly AsyncLimiter _limiter = new AsyncLimiter(10);

      public AwsS3DirectoryBrowser(AmazonS3Client client, string bucketName)
      {
         _client = client;
         _bucketName = bucketName;
      }

      public async Task<IReadOnlyCollection<Blob>> ListAsync(ListOptions options, CancellationToken cancellationToken)
      {
         var container = new List<Blob>();

         await ListFolderAsync(container, options.FolderPath, options, cancellationToken).ConfigureAwait(false);

         return options.MaxResults == null
            ? container
            : container.Count > options.MaxResults.Value
               ? container.Take(options.MaxResults.Value).ToList()
               : container;
      }

      private async Task ListFolderAsync(List<Blob> container, string path, ListOptions options, CancellationToken cancellationToken)
      {
         var request = new ListObjectsV2Request()
         {
            BucketName = _bucketName,
            Prefix = FormatFolderPrefix(path),
            Delimiter = "/"   //this tells S3 not to go into the folder recursively
         };

         var folderContainer = new List<Blob>();

         while(options.MaxResults == null || (container.Count < options.MaxResults))
         {
            ListObjectsV2Response response;

            using(await _limiter.AcquireOneAsync().ConfigureAwait(false))
            {
               response = await _client.ListObjectsV2Async(request, cancellationToken).ConfigureAwait(false);
            }

            folderContainer.AddRange(response.ToBlobs(options));

            if(response.NextContinuationToken == null)
               break;

            request.ContinuationToken = response.NextContinuationToken;
         }

         container.AddRange(folderContainer);

         if(options.Recurse)
         {
            List<Blob> folders = folderContainer.Where(b => b.Kind == BlobItemKind.Folder).ToList();

            await Task.WhenAll(folders.Select(f => ListFolderAsync(container, f.FullPath, options, cancellationToken))).ConfigureAwait(false);
         }
      }


      private static string FormatFolderPrefix(string folderPath)
      {
         folderPath = StoragePath.Normalize(folderPath).Substring(1);

         if(StoragePath.IsRootPath(folderPath))
            return null;

         if(!folderPath.EndsWith("/"))
            folderPath += "/";

         return folderPath;
      }


      public async Task DeleteRecursiveAsync(string fullPath, CancellationToken cancellationToken)
      {
         var request = new ListObjectsV2Request()
         {
            BucketName = _bucketName,
            Prefix = fullPath + "/"
         };

         while(true)
         {
            ListObjectsV2Response response = await _client.ListObjectsV2Async(request, cancellationToken).ConfigureAwait(false);

            await Task.WhenAll(response.S3Objects.Select(s3 => _client.DeleteObjectAsync(_bucketName, s3.Key, cancellationToken))).ConfigureAwait(false);

            if(response.NextContinuationToken == null)
               break;

            request.ContinuationToken = response.NextContinuationToken;
         }
      }

      public void Dispose()
      {
         _limiter.Dispose();
      }
   }
}