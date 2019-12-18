using System;
using Storage.Net.Amazon.Aws.Blobs;
using Storage.Net.Blobs;
using Storage.Net.ConnectionString;
using Storage.Net.Messaging;

namespace Storage.Net.Amazon.Aws
{
   class AwsStorageModule : IExternalModule, IConnectionFactory
   {
      public IConnectionFactory ConnectionFactory => this;

      public IBlobStorage CreateBlobStorage(StorageConnectionString connectionString)
      {
         if(connectionString.Prefix == KnownPrefix.AwsS3)
         {
            string cliProfileName = connectionString.Get(KnownParameter.LocalProfileName);
            connectionString.GetRequired(KnownParameter.BucketName, true, out string bucket);
            connectionString.GetRequired(KnownParameter.Region, true, out string region);

            if(string.IsNullOrEmpty(cliProfileName))
            {
               string keyId = connectionString.Get(KnownParameter.KeyId);
               string key = connectionString.Get(KnownParameter.KeyOrPassword);

               if(string.IsNullOrEmpty(keyId) != string.IsNullOrEmpty(key))
               {
                  throw new ArgumentException($"connection string requires both 'key' and 'keyId' parameters, or neither.");
               }


               if(string.IsNullOrEmpty(keyId))
               {
                  return new AwsS3BlobStorage(bucket, region);
               }

               string sessionToken = connectionString.Get(KnownParameter.SessionToken);
               return new AwsS3BlobStorage(keyId, key, sessionToken, bucket, region, null);
            }
#if !NET16
            else
            {
               return AwsS3BlobStorage.FromAwsCliProfile(cliProfileName, bucket, region);
            }
#endif
         }


         return null;
      }

      public IMessenger CreateMessenger(StorageConnectionString connectionString) => null;
   }
}
