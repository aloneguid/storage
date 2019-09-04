using System;
using System.Collections.Generic;
using System.Text;
using Storage.Net.Amazon.Aws.Blobs;
using Xunit;

namespace Storage.Net.Tests.Integration.Azure
{
   [Trait("Category", "Blobs")]
   public class LeakyAmazonS3StorageTest
   {
      private readonly ITestSettings _settings;
      private readonly IAwsS3BlobStorage _storage;

      public LeakyAmazonS3StorageTest()
      {
         _settings = Settings.Instance;

         _storage = (IAwsS3BlobStorage)StorageFactory.Blobs.AmazonS3BlobStorage(
            _settings.AwsAccessKeyId, _settings.AwsSecretAccessKey, _settings.AwsTestBucketName);
      }
   }
}
