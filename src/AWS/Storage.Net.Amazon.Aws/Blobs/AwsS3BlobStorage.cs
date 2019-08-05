using Storage.Net.Blobs;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Amazon;
using Amazon.S3;
using Amazon.Runtime;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using System.Threading.Tasks;
using System.Threading;
using Storage.Net.Streaming;
using NetBox.Extensions;

namespace Storage.Net.Amazon.Aws.Blobs
{
   /// <summary>
   /// Amazon S3 storage adapter for blobs
   /// </summary>
   class AwsS3BlobStorage : IBlobStorage, IAwsS3BlobStorage
   {
      private const int ListChunkSize = 10;
      private readonly string _bucketName;
      private readonly AmazonS3Client _client;
      private readonly TransferUtility _fileTransferUtility;
      private readonly bool _skipBucketCreation = false;
      private bool _initialised = false;
      

      /// <summary>
      /// Returns reference to the native AWS S3 blob client.
      /// </summary>
      public IAmazonS3 NativeBlobClient => _client;

      //https://github.com/awslabs/aws-sdk-net-samples/blob/master/ConsoleSamples/AmazonS3Sample/AmazonS3Sample/S3Sample.cs


      /// <summary>
      /// Creates a new instance of <see cref="AwsS3BlobStorage"/> for a given region endpoint, and will assume the runnning AWS ECS Task role credentials or Lambda role credentials />
      /// </summary>
      public AwsS3BlobStorage(string bucketName, RegionEndpoint regionEndpoint, bool skipBucketCreation = false)
      {
         _bucketName = bucketName ?? throw new ArgumentNullException(nameof(bucketName));
         _client = new AmazonS3Client(regionEndpoint);
         _skipBucketCreation = skipBucketCreation;
         _fileTransferUtility = new TransferUtility(_client);
      }


      /// <summary>
      /// Creates a new instance of <see cref="AwsS3BlobStorage"/> for a given region endpoint/>
      /// </summary>
      public AwsS3BlobStorage(string accessKeyId, string secretAccessKey, string bucketName, RegionEndpoint regionEndpoint, bool skipBucketCreation = false)
         : this(accessKeyId, secretAccessKey, bucketName, new AmazonS3Config { RegionEndpoint = regionEndpoint ?? RegionEndpoint.EUWest1 }, skipBucketCreation)
      {
      }

      /// <summary>
      /// Creates a new instance of <see cref="AwsS3BlobStorage"/> for an S3-compatible storage provider hosted on an alternative service URL/>
      /// </summary>
      public AwsS3BlobStorage(string accessKeyId, string secretAccessKey, string bucketName, string serviceUrl, bool skipBucketCreation = false)
         : this(accessKeyId, secretAccessKey, bucketName, new AmazonS3Config
         {
            RegionEndpoint = RegionEndpoint.USEast1,
            ServiceURL = serviceUrl
         }, skipBucketCreation)
      {
      }

      /// <summary>
      /// Creates a new instance of <see cref="AwsS3BlobStorage"/> for a given S3 client configuration/>
      /// </summary>
      public AwsS3BlobStorage(string accessKeyId, string secretAccessKey, string bucketName, AmazonS3Config clientConfig, bool skipBucketCreation = false)
      {
         if (accessKeyId == null) throw new ArgumentNullException(nameof(accessKeyId));
         if (secretAccessKey == null) throw new ArgumentNullException(nameof(secretAccessKey));
         _bucketName = bucketName ?? throw new ArgumentNullException(nameof(bucketName));
         _skipBucketCreation = skipBucketCreation;
         _client = new AmazonS3Client(new BasicAWSCredentials(accessKeyId, secretAccessKey), clientConfig);
         _fileTransferUtility = new TransferUtility(_client);
      }

      private async Task<AmazonS3Client> GetClientAsync()
      {
         if (!_initialised && !_skipBucketCreation)
         {
            try
            {
               var request = new PutBucketRequest { BucketName = _bucketName };

               await _client.PutBucketAsync(request);

               _initialised = true;
            }
            catch (AmazonS3Exception ex) when (ex.ErrorCode == "BucketAlreadyOwnedByYou")
            {
               //ignore this error as bucket already exists
            }
         }

         return _client;
      }

      /// <summary>
      /// Lists all buckets, optionaly filtering by prefix. Prefix filtering happens on client side.
      /// </summary>
      public async Task<IReadOnlyCollection<Blob>> ListAsync(ListOptions options = null, CancellationToken cancellationToken = default)
      {
         if (options == null) options = new ListOptions();

         GenericValidation.CheckBlobPrefix(options.FilePrefix);

         AmazonS3Client client = await GetClientAsync().ConfigureAwait(false);

         IReadOnlyCollection<Blob> blobs;
         using(var browser = new AwsS3DirectoryBrowser(client, _bucketName))
         {
            blobs = await browser.ListAsync(options, cancellationToken).ConfigureAwait(false);
         }

         if(options.IncludeAttributes)
         {
            foreach(IEnumerable<Blob> page in blobs.Where(b => !b.IsFolder).Chunk(ListChunkSize))
            {
               await Converter.AppendMetadataAsync(client, _bucketName, page, cancellationToken).ConfigureAwait(false);
            }
         }

         return blobs;
      }

      /// <summary>
      /// S3 doesnt support this natively and will cache everything in MemoryStream until disposed.
      /// </summary>
      public Task<Stream> OpenWriteAsync(string fullPath, bool append = false, CancellationToken cancellationToken = default)
      {
         if (append) throw new NotSupportedException();
         GenericValidation.CheckBlobFullPath(fullPath);
         fullPath = StoragePath.Normalize(fullPath, false);

         //http://docs.aws.amazon.com/AmazonS3/latest/dev/HLuploadFileDotNet.html

         var callbackStream = new FixedStream(new MemoryStream(), null, async (fx) =>
         {
            var ms = (MemoryStream)fx.Parent;
            ms.Position = 0;

            await _fileTransferUtility.UploadAsync(ms, _bucketName, fullPath, cancellationToken).ConfigureAwait(false);
         });

         return Task.FromResult<Stream>(callbackStream);
      }

      public async Task<Stream> OpenReadAsync(string fullPath, CancellationToken cancellationToken = default)
      {
         GenericValidation.CheckBlobFullPath(fullPath);

         fullPath = StoragePath.Normalize(fullPath, false);
         GetObjectResponse response = await GetObjectAsync(fullPath).ConfigureAwait(false);
         if (response == null) return null;

         return new FixedStream(response.ResponseStream, length: response.ContentLength, (Action<FixedStream>)null);
      }

      public async Task DeleteAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default)
      {
         AmazonS3Client client = await GetClientAsync().ConfigureAwait(false);

         await Task.WhenAll(fullPaths.Select(fullPath => DeleteAsync(fullPath, client, cancellationToken))).ConfigureAwait(false);
      }

      private async Task DeleteAsync(string fullPath, AmazonS3Client client, CancellationToken cancellationToken = default)
      {
         GenericValidation.CheckBlobFullPath(fullPath);

         fullPath = StoragePath.Normalize(fullPath, false);
         
         await client.DeleteObjectAsync(_bucketName, fullPath, cancellationToken).ConfigureAwait(false);
         using(var browser = new AwsS3DirectoryBrowser(client, _bucketName))
         {
            await browser.DeleteRecursiveAsync(fullPath, cancellationToken).ConfigureAwait(false);
         }
      }

      public async Task<IReadOnlyCollection<bool>> ExistsAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default)
      {
         return await Task.WhenAll(fullPaths.Select(ExistsAsync));
      }

      private async Task<bool> ExistsAsync(string fullPath)
      {
         GenericValidation.CheckBlobFullPath(fullPath);

         try
         {
            fullPath = StoragePath.Normalize(fullPath, false);
            using (GetObjectResponse response = await GetObjectAsync(fullPath).ConfigureAwait(false))
            {
               if (response == null) return false;
            }
         }
         catch (StorageException ex)
         {
            if (ex.ErrorCode == ErrorCode.NotFound) return false;
         }

         return true;
      }

      public async Task<IReadOnlyCollection<Blob>> GetBlobsAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default)
      {
         Blob[] blobs = await Task.WhenAll(fullPaths.Select(GetBlobAsync)).ConfigureAwait(false);

         await Converter.AppendMetadataAsync(
            await GetClientAsync().ConfigureAwait(false),
            _bucketName,
            blobs,
            cancellationToken).ConfigureAwait(false);

         return blobs;
      }

      private async Task<Blob> GetBlobAsync(string fullPath)
      {
         GenericValidation.CheckBlobFullPath(fullPath);

         try
         {
            fullPath = StoragePath.Normalize(fullPath, false);
            using (GetObjectResponse obj = await GetObjectAsync(fullPath).ConfigureAwait(false))
            {
               //ETag contains actual MD5 hash, not sure why!

               if(obj != null)
               {
                  var r = new Blob(fullPath);
                  r.MD5 = obj.ETag.Trim('\"');
                  r.Size = obj.ContentLength;
                  r.LastModificationTime = obj.LastModified.ToUniversalTime();
                  return r;
               }
            }
         }
         catch (StorageException ex) when (ex.ErrorCode == ErrorCode.NotFound)
         {
            //if blob is not found, don't return any information
         }

         return null;
      }

      public async Task SetBlobsAsync(IEnumerable<Blob> blobs, CancellationToken cancellationToken = default)
      {
         if(blobs == null)
            return;

         AmazonS3Client client = await GetClientAsync().ConfigureAwait(false);

         foreach(Blob blob in blobs.Where(b => b != null))
         {
            if(blob.Metadata != null)
            {
               await Converter.UpdateMetadataAsync(
                  client,
                  blob,
                  _bucketName,
                  blob).ConfigureAwait(false);
            }
         }
      }

      private async Task<GetObjectResponse> GetObjectAsync(string key)
      {
         var request = new GetObjectRequest { BucketName = _bucketName, Key = key };
         AmazonS3Client client = await GetClientAsync().ConfigureAwait(false);

         try
         {
            GetObjectResponse response = await client.GetObjectAsync(request).ConfigureAwait(false);
            return response;
         }
         catch (AmazonS3Exception ex)
         {
            if (IsDoesntExist(ex)) return null;

            TryHandleException(ex);
            throw;
         }
      }


      private static bool TryHandleException(AmazonS3Exception ex)
      {
         if (IsDoesntExist(ex))
         {
            throw new StorageException(ErrorCode.NotFound, ex);
         }

         return false;
      }

      private static bool IsDoesntExist(AmazonS3Exception ex)
      {
         return ex.ErrorCode == "NoSuchKey";
      }

      public void Dispose()
      {
      }

      public Task<ITransaction> OpenTransactionAsync()
      {
         return Task.FromResult(EmptyTransaction.Instance);
      }

      /// <summary>
      /// Get presigned url for upload object to Blob Storage.
      /// </summary>
      public async Task<string> GetUploadUrlAsync(string fullPath, string mimeType, int expiresInSeconds = 86000)
      {
         return await GetPresignedUrlAsync(fullPath, mimeType, expiresInSeconds, HttpVerb.PUT);
      }

      /// <summary>
      /// Get presigned url for download object from Blob Storage.
      /// </summary>
      public async Task<string> GetDownloadUrlAsync(string fullPath, string mimeType, int expiresInSeconds = 86000)
      {
         return await GetPresignedUrlAsync(fullPath, mimeType, expiresInSeconds, HttpVerb.GET);
      }

      /// <summary>
      /// Get presigned url for requested operation with Blob Storage.
      /// </summary>
      public async Task<string> GetPresignedUrlAsync(string fullPath, string mimeType, int expiresInSeconds, HttpVerb verb)
      {
         IAmazonS3 client = await GetClientAsync();

         return client.GetPreSignedURL(new GetPreSignedUrlRequest()
         {
            BucketName = _bucketName,
            ContentType = mimeType,
            Expires = DateTime.UtcNow.AddSeconds(expiresInSeconds),
            Key = StoragePath.Normalize(fullPath, false),
            Verb = verb,
         });
      }
   }
}
