using Amazon;
using Amazon.S3;
using Storage.Net.Amazon.Aws.Messaging;
using Storage.Net.Amazon.Aws.Blobs;
using Storage.Net.Blobs;
using Storage.Net.Messaging;
using Storage.Net.Amazon.Aws;
using Storage.Net.ConnectionString;
using Amazon.S3.Transfer;

namespace Storage.Net
{
   /// <summary>
   /// Factory class that implement factory methods for Amazon AWS implementation
   /// </summary>
   public static class Factory
   {

      /// <summary>
      /// Register Azure module.
      /// </summary>
      public static IModulesFactory UseAwsStorage(this IModulesFactory factory)
      {
         return factory.Use(new AwsStorageModule());
      }


      /// <summary>
      /// Creates an Amazon S3 storage using assumed role permissions (useful when running the code wform within ECS tasks or lambda where you don't need to provide and manage accessKeys and secrets as the permissions are assumed via the IAM role the lambda or ecs tasks has assigned to it)
      /// </summary>
      /// <param name="factory">Factory reference</param>
      /// <param name="bucketName">Bucket name</param>
      /// <param name="region">Required regional endpoint.</param>
      /// <returns>A reference to the created storage</returns>
      public static IBlobStorage AwsS3(this IBlobStorageFactory factory,
         string bucketName,
         string region)
      {
         return new AwsS3BlobStorage(bucketName, region);
      }

      /// <summary>
      /// Creates an Amazon S3 storage
      /// </summary>
      /// <param name="factory">Factory reference</param>
      /// <param name="accessKeyId">Access key ID</param>
      /// <param name="secretAccessKey">Secret access key</param>
      /// /// <param name="sessionToken">Optional. Only required when using session credentials.</param>
      /// <param name="bucketName">Bucket name</param>
      /// <param name="region">Region endpoint</param>
      /// <param name="serviceUrl">S3-compatible service location</param>
      /// <returns>A reference to the created storage</returns>
      public static IBlobStorage AwsS3(this IBlobStorageFactory factory,
         string accessKeyId,
         string secretAccessKey,
         string sessionToken,
         string bucketName,
         string region,
         string serviceUrl = null)
      {
         return new AwsS3BlobStorage(accessKeyId, secretAccessKey, sessionToken, bucketName, region, serviceUrl);
      }

      /// <summary>
      /// Creates an Amazon S3 storage provider for a custom S3-compatible storage server
      /// </summary>
      /// <param name="factory">Factory reference</param>
      /// <param name="accessKeyId">Access key ID</param>
      /// <param name="secretAccessKey">Secret access key</param>
      /// <param name="sessionToken">Optional. Only required when using session credentials.</param>
      /// <param name="bucketName">Bucket name</param>
      /// <param name="clientConfig">S3 client configuration</param>
      /// <param name="transferUtilityConfig">S3 transfer utility configuration</param>
      /// <returns>A reference to the created storage</returns>
      public static IBlobStorage AwsS3(this IBlobStorageFactory factory,
         string accessKeyId,
         string secretAccessKey,
         string sessionToken,
         string bucketName,
         AmazonS3Config clientConfig,
         TransferUtilityConfig transferUtilityConfig = null)
      {
         return new AwsS3BlobStorage(accessKeyId, secretAccessKey, sessionToken, bucketName, clientConfig, transferUtilityConfig);
      }

#if !NET16

      /// <summary>
      /// Creates an Amazon S3 storage provider using credentials from AWS CLI configuration file (~/.aws/credentials)
      /// </summary>
      /// <param name="factory">Factory reference</param>
      /// <param name="awsCliProfileName"></param>
      /// <param name="bucketName">Bucket name</param>
      /// <param name="region"></param>
      /// <returns>A reference to the created storage</returns>
      public static IBlobStorage AwsS3(this IBlobStorageFactory factory,
         string awsCliProfileName,
         string bucketName,
         string region)
      {
         return AwsS3BlobStorage.FromAwsCliProfile(awsCliProfileName, bucketName, region);
      }
      
      /// <summary>
      /// Creates an Amazon S3 storage provider using credentials retrieve from SSO
      /// </summary>
      /// <param name="factory">Factory reference</param>
      /// <param name="credentials"></param>
      /// <param name="bucketName">Bucket name</param>
      /// <param name="region"></param>
      /// <returns>A reference to the created storage</returns>
      public static IBlobStorage AwsS3(this IBlobStorageFactory factory,
         AWSCredentials credentials,
         string bucketName,
         string region)
      {
         return AwsS3BlobStorage.FromAwsCredentials(credentials, bucketName, region);
      }
#endif

      /// <summary>
      /// Creates Amazon Simple Queue Service publisher
      /// </summary>
      /// <param name="factory"></param>
      /// <param name="accessKeyId">Access key ID</param>
      /// <param name="secretAccessKey">Secret access key</param>
      /// <param name="serviceUrl"></param>
      /// <param name="regionEndpoint"></param>
      /// <returns></returns>
      public static IMessenger AwsSQS(this IMessagingFactory factory,
         string accessKeyId,
         string secretAccessKey,
         string serviceUrl,
         RegionEndpoint regionEndpoint = null)
      {
         return new AwsSQSMessenger(accessKeyId, secretAccessKey, serviceUrl, regionEndpoint);
      }

      #region [ Connection Strings ]

      /// <summary>
      /// Creates a connection string from AWS CLI profile name
      /// </summary>
      /// <param name="factory"></param>
      /// <param name="profileName"></param>
      /// <param name="bucketName"></param>
      /// <param name="region"></param>
      /// <returns></returns>
      public static StorageConnectionString ForAwsS3FromCliProfile(this IConnectionStringFactory factory,
         string profileName,
         string bucketName,
         string region)
      {
         if(profileName is null)
            throw new System.ArgumentNullException(nameof(profileName));
         if(bucketName is null)
            throw new System.ArgumentNullException(nameof(bucketName));
         if(region is null)
            throw new System.ArgumentNullException(nameof(region));
         var cs = new StorageConnectionString(KnownPrefix.AwsS3 + "://");
         cs[KnownParameter.LocalProfileName] = profileName;
         cs[KnownParameter.BucketName] = bucketName;
         cs[KnownParameter.Region] = region;
         return cs;
      }

      #endregion
   }
}
