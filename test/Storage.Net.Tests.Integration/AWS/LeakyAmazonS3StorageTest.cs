﻿using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Storage.Net.Amazon.Aws.Blobs;
using Storage.Net.Blobs;
using Xunit;

namespace Storage.Net.Tests.Integration.AWS
{
   [Trait("Category", "Blobs")]
   public class LeakyAmazonS3StorageTest
   {
      private readonly ITestSettings _settings;
      private readonly IAwsS3BlobStorage _storage;

      public LeakyAmazonS3StorageTest()
      {
         _settings = Settings.Instance;

         _storage = (IAwsS3BlobStorage)StorageFactory.Blobs.AwsS3(
            _settings.AwsAccessKeyId, _settings.AwsSecretAccessKey, _settings.AwsTestBucketName);
      }

      [Fact]
      public async Task Connect_NoRegionEndpoint()
      {
         IBlobStorage storage = StorageFactory.Blobs.AwsS3(
            _settings.AwsAccessKeyId, _settings.AwsSecretAccessKey, _settings.AwsTestBucketName);

         await storage.ListAsync();
      }

      [Fact]
      public async Task ListBuckets_WithPermissions_Lists()
      {
         IBlobStorage storage = StorageFactory.Blobs.AwsS3(
            _settings.AwsAccessKeyId, _settings.AwsSecretAccessKey);

         await storage.ListAsync();
      }
   }
}
