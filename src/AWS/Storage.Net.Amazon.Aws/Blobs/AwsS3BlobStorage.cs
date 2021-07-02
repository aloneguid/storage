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
using System.Net;

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
      private bool _initialised = false;


      /// <summary>
      /// Returns reference to the native AWS S3 blob client.
      /// </summary>
      public IAmazonS3 NativeBlobClient => _client;

      //https://github.com/awslabs/aws-sdk-net-samples/blob/master/ConsoleSamples/AmazonS3Sample/AmazonS3Sample/S3Sample.cs


#if !NET16
      public static AwsS3BlobStorage FromAwsCliProfile(string profileName, string bucketName, string region)
      {
         return new AwsS3BlobStorage(bucketName, region, AwsCliCredentials.GetCredentials(profileName));
      }
      
      public static AwsS3BlobStorage FromAwsCredentials(AwsCredentials credentials, string bucketName, string region)
      {
         return new AwsS3BlobStorage(bucketName, region, credentials);
      }
#endif

      public AwsS3BlobStorage(string bucketName, string region, AWSCredentials credentials)
      {
         _bucketName = bucketName;
         _client = new AmazonS3Client(credentials, CreateConfig(region, null));
         _fileTransferUtility = new TransferUtility(_client);
      }

      /// <summary>
      /// Creates a new instance of <see cref="AwsS3BlobStorage"/> for a given region endpoint, and will assume the running AWS ECS Task role credentials or Lambda role credentials
      /// </summary>
      public AwsS3BlobStorage(string bucketName, string region)
      {
         _bucketName = bucketName ?? throw new ArgumentNullException(nameof(bucketName));
         _client = new AmazonS3Client(region.ToRegionEndpoint());
         _fileTransferUtility = new TransferUtility(_client);
      }

      /// <summary>
      /// Creates a new instance of <see cref="AwsS3BlobStorage"/> for a given region endpoint
      /// </summary>
      public AwsS3BlobStorage(string accessKeyId, string secretAccessKey, string sessionToken, string bucketName, string region, string serviceUrl)
         : this(accessKeyId, secretAccessKey, sessionToken, bucketName, CreateConfig(region, serviceUrl))
      {
      }

      private static AmazonS3Config CreateConfig(string region, string serviceUrl)
      {
         var config = new AmazonS3Config();
         if(region != null)
            config.RegionEndpoint = region.ToRegionEndpoint();
         if(serviceUrl != null)
            config.ServiceURL = serviceUrl;
         return config;
      }

      /// <summary>
      /// Creates a new instance of <see cref="AwsS3BlobStorage"/> for a given S3 client configuration
      /// </summary>
      public AwsS3BlobStorage(string accessKeyId, string secretAccessKey, string sessionToken,
         string bucketName, AmazonS3Config clientConfig)
         : this(accessKeyId, secretAccessKey, sessionToken, bucketName, clientConfig, new TransferUtilityConfig())
      {
      }

      /// <summary>
      /// Creates a new instance of <see cref="AwsS3BlobStorage"/> for a given S3 client configuration
      /// </summary>
      public AwsS3BlobStorage(string accessKeyId, string secretAccessKey, string sessionToken,
         string bucketName, AmazonS3Config clientConfig, TransferUtilityConfig transferUtilityConfig)
      {
         if(accessKeyId == null)
            throw new ArgumentNullException(nameof(accessKeyId));
         if(secretAccessKey == null)
            throw new ArgumentNullException(nameof(secretAccessKey));
         _bucketName = bucketName ?? throw new ArgumentNullException(nameof(bucketName));

         AWSCredentials awsCreds = (sessionToken == null)
            ? (AWSCredentials)new BasicAWSCredentials(accessKeyId, secretAccessKey)
            : new SessionAWSCredentials(accessKeyId, secretAccessKey, sessionToken);

         _client = new AmazonS3Client(awsCreds, clientConfig);

         _fileTransferUtility = new TransferUtility(_client, transferUtilityConfig ?? new TransferUtilityConfig());
      }

      private async Task<AmazonS3Client> GetClientAsync()
      {
         if(!_initialised)
         {
            try
            {
               var request = new PutBucketRequest { BucketName = _bucketName };

               await _client.PutBucketAsync(request).ConfigureAwait(false);

               _initialised = true;
            }
            catch(AmazonS3Exception ex) when(ex.ErrorCode == "BucketAlreadyOwnedByYou")
            {
               //ignore this error as bucket already exists
               _initialised = true;
            }
         }

         return _client;
      }

      /// <summary>
      /// Lists all buckets, optionaly filtering by prefix. Prefix filtering happens on client side.
      /// </summary>
      public async Task<IReadOnlyCollection<Blob>> ListAsync(ListOptions options = null, CancellationToken cancellationToken = default)
      {
         if(options == null)
            options = new ListOptions();

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
      public async Task WriteAsync(string fullPath, Stream dataStream, bool append = false,
         CancellationToken cancellationToken = default)
      {
         if(append)
            throw new NotSupportedException();
         GenericValidation.CheckBlobFullPath(fullPath);
         fullPath = StoragePath.Normalize(fullPath, true);

         //http://docs.aws.amazon.com/AmazonS3/latest/dev/HLuploadFileDotNet.html

         await _fileTransferUtility.UploadAsync(dataStream, _bucketName, fullPath, cancellationToken).ConfigureAwait(false);
      }

      public async Task<Stream> OpenReadAsync(string fullPath, CancellationToken cancellationToken = default)
      {
         GenericValidation.CheckBlobFullPath(fullPath);

         fullPath = StoragePath.Normalize(fullPath, true);
         GetObjectResponse response = await GetObjectAsync(fullPath).ConfigureAwait(false);
         if(response == null)
            return null;

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

         fullPath = StoragePath.Normalize(fullPath, true);

         await client.DeleteObjectAsync(_bucketName, fullPath, cancellationToken).ConfigureAwait(false);
         using(var browser = new AwsS3DirectoryBrowser(client, _bucketName))
         {
            await browser.DeleteRecursiveAsync(fullPath, cancellationToken).ConfigureAwait(false);
         }
      }

      public async Task<IReadOnlyCollection<bool>> ExistsAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default)
      {
         AmazonS3Client client = await GetClientAsync().ConfigureAwait(false);

         return await Task.WhenAll(fullPaths.Select(fullPath => ExistsAsync(client, fullPath, cancellationToken))).ConfigureAwait(false);
      }

      private async Task<bool> ExistsAsync(AmazonS3Client client, string fullPath, CancellationToken cancellationToken)
      {
         GenericValidation.CheckBlobFullPath(fullPath);

         try
         {
            fullPath = StoragePath.Normalize(fullPath, true);
            await client.GetObjectMetadataAsync(_bucketName, fullPath, cancellationToken).ConfigureAwait(false);
            return true;
         }
         catch(AmazonS3Exception ex) when(ex.StatusCode == HttpStatusCode.NotFound)
         {

         }

         return false;
      }

      public async Task<IReadOnlyCollection<Blob>> GetBlobsAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default)
      {
         return await Task.WhenAll(fullPaths.Select(GetBlobAsync)).ConfigureAwait(false);
      }

      private async Task<Blob> GetBlobAsync(string fullPath)
      {
         GenericValidation.CheckBlobFullPath(fullPath);
         fullPath = StoragePath.Normalize(fullPath, true);

         AmazonS3Client client = await GetClientAsync().ConfigureAwait(false);

         try
         {
            GetObjectMetadataResponse meta = await client.GetObjectMetadataAsync(_bucketName, fullPath).ConfigureAwait(false);
            return meta.ToBlob(fullPath);
         }
         catch(AmazonS3Exception ex) when(ex.StatusCode == HttpStatusCode.NotFound)
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
                  StoragePath.Normalize(blob.FullPath, true)).ConfigureAwait(false);
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
         catch(AmazonS3Exception ex)
         {
            if(IsDoesntExist(ex))
               return null;

            TryHandleException(ex);
            throw;
         }
      }


      private static bool TryHandleException(AmazonS3Exception ex)
      {
         if(IsDoesntExist(ex))
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
         return await GetPresignedUrlAsync(fullPath, mimeType, expiresInSeconds, HttpVerb.PUT).ConfigureAwait(false);
      }

      /// <summary>
      /// Get presigned url for download object from Blob Storage.
      /// </summary>
      public async Task<string> GetDownloadUrlAsync(string fullPath, string mimeType, int expiresInSeconds = 86000)
      {
         return await GetPresignedUrlAsync(fullPath, mimeType, expiresInSeconds, HttpVerb.GET).ConfigureAwait(false);
      }

      /// <summary>
      /// Get presigned url for requested operation with Blob Storage.
      /// </summary>
      public async Task<string> GetPresignedUrlAsync(string fullPath, string mimeType, int expiresInSeconds, HttpVerb verb)
      {
         IAmazonS3 client = await GetClientAsync().ConfigureAwait(false);

         return client.GetPreSignedURL(new GetPreSignedUrlRequest()
         {
            BucketName = _bucketName,
            ContentType = mimeType,
            Expires = DateTime.UtcNow.AddSeconds(expiresInSeconds),
            Key = StoragePath.Normalize(fullPath, true),
            Verb = verb,
         });
      }
   }
}
