using Amazon;
using Amazon.S3;
using Storage.Net.Amazon.Aws.Messaging;
using Storage.Net.Aws.Blob;
using Storage.Net.Blob;
using Storage.Net.Messaging;

namespace Storage.Net
{
   /// <summary>
   /// Factory class that implement factory methods for Amazon AWS implementation
   /// </summary>
   public static class Factory
   {

      /// <summary>
      /// Creates an Amazon S3 storage
      /// </summary>
      /// <param name="factory">Factory reference</param>
      /// <param name="accessKeyId">Access key ID</param>
      /// <param name="secretAccessKey">Secret access key</param>
      /// <param name="bucketName">Bucket name</param>
      /// <param name="regionEndpoint">Optionally set region endpoint. When not specified defaults to EU West</param>
      /// <returns>A reference to the created storage</returns>
      public static IBlobStorage AmazonS3BlobStorage(this IBlobStorageFactory factory,
         string accessKeyId,
         string secretAccessKey,
         string bucketName,
         RegionEndpoint regionEndpoint = null)
      {
         return new AwsS3BlobStorageProvider(accessKeyId, secretAccessKey, bucketName, regionEndpoint);
      }

      /// <summary>
      /// Creates an Amazon S3 storage provider for a custom S3-compatible storage server
      /// </summary>
      /// <param name="factory">Factory reference</param>
      /// <param name="accessKeyId">Access key ID</param>
      /// <param name="secretAccessKey">Secret access key</param>
      /// <param name="bucketName">Bucket name</param>
      /// <param name="serviceUrl">S3-compatible service location</param>
      /// <returns>A reference to the created storage</returns>
      public static IBlobStorage AmazonS3BlobStorage(this IBlobStorageFactory factory,
         string accessKeyId,
         string secretAccessKey,
         string bucketName,
         string serviceUrl)
      {
         return new AwsS3BlobStorageProvider(accessKeyId, secretAccessKey, bucketName, serviceUrl);
      }

      /// <summary>
      /// Creates an Amazon S3 storage provider for a custom S3-compatible storage server
      /// </summary>
      /// <param name="factory">Factory reference</param>
      /// <param name="accessKeyId">Access key ID</param>
      /// <param name="secretAccessKey">Secret access key</param>
      /// <param name="bucketName">Bucket name</param>
      /// <param name="clientConfig">S3 client configuration</param>
      /// <returns>A reference to the created storage</returns>
      public static IBlobStorage AmazonS3BlobStorage(this IBlobStorageFactory factory,
         string accessKeyId,
         string secretAccessKey,
         string bucketName,
         AmazonS3Config clientConfig)
      {
         return new AwsS3BlobStorageProvider(accessKeyId, secretAccessKey, bucketName, clientConfig);
      }

      /// <summary>
      /// Creates Amazon Simple Queue Service publisher
      /// </summary>
      /// <param name="factory"></param>
      /// <param name="accessKeyId">Access key ID</param>
      /// <param name="secretAccessKey">Secret access key</param>
      /// <param name="serviceUrl"></param>
      /// <param name="queueName"></param>
      /// <param name="regionEndpoint"></param>
      /// <returns></returns>
      public static IMessagePublisher AmazonSQSMessagePublisher(this IMessagingFactory factory,
         string accessKeyId,
         string secretAccessKey,
         string serviceUrl,
         string queueName,
         RegionEndpoint regionEndpoint = null)
      {
         return new AwsS3MessagePublisher(accessKeyId, secretAccessKey, serviceUrl, queueName, regionEndpoint);
      }

      /// <summary>
      /// Creates Amazon Simple Queue Service receiver
      /// </summary>
      /// <param name="factory"></param>
      /// <param name="accessKeyId">Access key ID</param>
      /// <param name="secretAccessKey">Secret access key</param>
      /// <param name="serviceUrl"></param>
      /// <param name="queueName"></param>
      /// <param name="regionEndpoint"></param>
      /// <returns></returns>
      public static IMessageReceiver AmazonSQSMessageReceiver(this IMessagingFactory factory,
         string accessKeyId,
         string secretAccessKey,
         string serviceUrl,
         string queueName,
         RegionEndpoint regionEndpoint = null)
      {
         return new AwsS3MessageReceiver(accessKeyId, secretAccessKey, serviceUrl, queueName, regionEndpoint);
      }
   }
}
