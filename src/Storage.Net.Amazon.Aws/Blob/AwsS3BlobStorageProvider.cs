using Storage.Net.Blob;
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

namespace Storage.Net.Aws.Blob
{
   /// <summary>
   /// Amazon S3 storage adapter for blobs
   /// </summary>
   class AwsS3BlobStorageProvider : IBlobStorage
   {
      private readonly string _bucketName;
      private readonly AmazonS3Client _client;
      private readonly TransferUtility _fileTransferUtility;

      //https://github.com/awslabs/aws-sdk-net-samples/blob/master/ConsoleSamples/AmazonS3Sample/AmazonS3Sample/S3Sample.cs

      /// <summary>
      /// Creates a new instance of <see cref="AwsS3BlobStorageProvider"/> for a given region endpoint/>
      /// </summary>
      public AwsS3BlobStorageProvider(string accessKeyId, string secretAccessKey, string bucketName, RegionEndpoint regionEndpoint)
         : this(accessKeyId, secretAccessKey, bucketName, new AmazonS3Config { RegionEndpoint = regionEndpoint ?? RegionEndpoint.EUWest1 })
      {
      }

      /// <summary>
      /// Creates a new instance of <see cref="AwsS3BlobStorageProvider"/> for an S3-compatible storage provider hosted on an alternative service URL/>
      /// </summary>
      public AwsS3BlobStorageProvider(string accessKeyId, string secretAccessKey, string bucketName, string serviceUrl)
         : this(accessKeyId, secretAccessKey, bucketName, new AmazonS3Config
         {
            RegionEndpoint = RegionEndpoint.USEast1,
            ServiceURL = serviceUrl
         })
      {
      }

      /// <summary>
      /// Creates a new instance of <see cref="AwsS3BlobStorageProvider"/> for a given S3 client configuration/>
      /// </summary>
      public AwsS3BlobStorageProvider(string accessKeyId, string secretAccessKey, string bucketName, AmazonS3Config clientConfig)
      {
         if(accessKeyId == null) throw new ArgumentNullException(nameof(accessKeyId));
         if(secretAccessKey == null) throw new ArgumentNullException(nameof(secretAccessKey));
         _bucketName = bucketName ?? throw new ArgumentNullException(nameof(bucketName));

         _client = new AmazonS3Client(new BasicAWSCredentials(accessKeyId, secretAccessKey), clientConfig);
         _fileTransferUtility = new TransferUtility(_client);

         Initialise();
      }


      private void Initialise()
      {
         try
         {
            var request = new PutBucketRequest { BucketName = _bucketName };

#if NETSTANDARD
            _client.PutBucketAsync(request).Wait();
#else
            _client.PutBucket(request);
#endif
         }
         catch(AmazonS3Exception ex)
         {
            if(ex.ErrorCode == "BucketAlreadyOwnedByYou")
            {
               //ignore this error as bucket already exists
            }
            else
            {
               throw;
            }
         }
      }

      /// <summary>
      /// Lists all buckets, optionaly filtering by prefix. Prefix filtering happens on client side.
      /// </summary>
      public async Task<IReadOnlyCollection<BlobId>> ListAsync(ListOptions options, CancellationToken cancellationToken)
      {
         if (options == null) options = new ListOptions();

         GenericValidation.CheckBlobPrefix(options.FilePrefix);

         var request = new ListObjectsV2Request()
         {
            BucketName = _bucketName,
            Prefix = options.FilePrefix ?? null
         };
         if (options.MaxResults.HasValue) request.MaxKeys = options.MaxResults.Value;


         //todo: paging
         ListObjectsV2Response response = await _client.ListObjectsV2Async(request, cancellationToken);

         return response.S3Objects
            .Select(s3Obj => new BlobId(StoragePath.RootFolderPath, s3Obj.Key, BlobItemKind.File))
            .Where(options.IsMatch)
            .Where(bid => (options.FolderPath == null || bid.FolderPath == options.FolderPath))
            .Where(bid => options.BrowseFilter == null || options.BrowseFilter(bid))
            .ToList();
      }

      public async Task WriteAsync(string id, Stream sourceStream, bool append, CancellationToken cancellationToken)
      {
         if (append) throw new NotSupportedException();

         GenericValidation.CheckBlobId(id);
         GenericValidation.CheckSourceStream(sourceStream);

         //http://docs.aws.amazon.com/AmazonS3/latest/dev/HLuploadFileDotNet.html

         id = StoragePath.Normalize(id, false);
         await _fileTransferUtility.UploadAsync(sourceStream, _bucketName, id, cancellationToken);
      }

      /// <summary>
      /// S3 doesnt support this natively and will cache everything in MemoryStream until disposed.
      /// </summary>
      public Task<Stream> OpenWriteAsync(string id, bool append, CancellationToken cancellationToken)
      {
         if (append) throw new NotSupportedException();
         GenericValidation.CheckBlobId(id);
         id = StoragePath.Normalize(id, false);

         var callbackStream = new FixedStream(new MemoryStream(), null, fx =>
         {
            var ms = (MemoryStream)fx.Parent;
            ms.Position = 0;

            _fileTransferUtility.UploadAsync(ms, _bucketName, id, cancellationToken).Wait();
         });

         return Task.FromResult<Stream>(callbackStream);
      }

      public async Task<Stream> OpenReadAsync(string id, CancellationToken cancellationToken)
      {
         GenericValidation.CheckBlobId(id);

         id = StoragePath.Normalize(id, false);
         GetObjectResponse response = await GetObjectAsync(id);
         if (response == null) return null;

         return new FixedStream(response.ResponseStream, length: response.ContentLength);
      }

      public Task DeleteAsync(IEnumerable<string> ids, CancellationToken cancellationToken)
      {
         return Task.WhenAll(ids.Select(id => DeleteAsync(id, cancellationToken)));
      }

      private Task DeleteAsync(string id, CancellationToken cancellationToken)
      {
         GenericValidation.CheckBlobId(id);

         id = StoragePath.Normalize(id, false);
         return _client.DeleteObjectAsync(_bucketName, id, cancellationToken);
      }

      public async Task<IReadOnlyCollection<bool>> ExistsAsync(IEnumerable<string> ids, CancellationToken cancellationToken)
      {
         return await Task.WhenAll(ids.Select(ExistsAsync));
      }

      private async Task<bool> ExistsAsync(string id)
      {
         GenericValidation.CheckBlobId(id);

         try
         {
            id = StoragePath.Normalize(id, false);
            using (GetObjectResponse response = await GetObjectAsync(id))
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

      public async Task<IEnumerable<BlobMeta>> GetMetaAsync(IEnumerable<string> ids, CancellationToken cancellationToken)
      {
         return await Task.WhenAll(ids.Select(GetMetaAsync));
      }

      private async Task<BlobMeta> GetMetaAsync(string id)
      {
         GenericValidation.CheckBlobId(id);
   
         try
         {
            id = StoragePath.Normalize(id, false);
            using (GetObjectResponse obj = await GetObjectAsync(id))
            {
               //ETag contains actual MD5 hash, not sure why!
   
               return (obj != null) 
                  ? new BlobMeta(obj.ContentLength, obj.ETag.Trim('\"'), obj.LastModified.ToUniversalTime())
                  : null;  
            }
         }
         catch (StorageException ex) when (ex.ErrorCode == ErrorCode.NotFound)
         {
            return null;
         }
      }

      private async Task<GetObjectResponse> GetObjectAsync(string key)
      {
         var request = new GetObjectRequest { BucketName = _bucketName, Key = key };

         try
         {
            GetObjectResponse response = await _client.GetObjectAsync(request);
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
   }
}
