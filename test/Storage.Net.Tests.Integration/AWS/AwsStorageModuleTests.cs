using System;
using Storage.Net.Amazon.Aws.Blobs;
using Storage.Net.Blobs;
using Xunit;

namespace Storage.Net.Tests.Integration.AWS
{
   public class AwsStorageModuleTests
   {
      private static readonly IBlobStorageFactory Factory = StorageFactory.Blobs;
      public AwsStorageModuleTests() => StorageFactory.Modules.UseAwsStorage();

      [Fact]
      public void FromConnectionString_WithBucketAndRegion_ReturnsAwsStorageModule()
      {
         IBlobStorage module = Factory.FromConnectionString("aws.s3://bucket=c;region=d");

         Assert.IsAssignableFrom<IAwsS3BlobStorage>(module);
      }

      [Fact]
      public void FromConnectionString_WithServiceUrl_WithoutRegion_ReturnsAwsStorageModule()
      {
         IBlobStorage module = Factory.FromConnectionString(
            "aws.s3://keyId=a;key=b;bucket=c;serviceUrl=https://s3.example.local"
         );

         Assert.IsAssignableFrom<IAwsS3BlobStorage>(module);
      }

      [Fact]
      public void FromConnectionString_WithoutServiceUrlAndRegion_ThrowsException() =>
         Assert.Throws<ArgumentException>(() => Factory.FromConnectionString("aws.s3://keyId=a;key=b;bucket=c;"));

      [Fact]
      public void FromConnectionString_WithoutKeyIdButNoKey_ThrowsException() =>
         Assert.Throws<ArgumentException>(() => Factory.FromConnectionString("aws.s3://keyId=a;bucket=c;region=d"));

      [Fact]
      public void FromConnectionString_WithoutKeyButNoKeyId_ThrowsException() =>
         Assert.Throws<ArgumentException>(() => Factory.FromConnectionString("aws.s3://key=b;bucket=c;region=d"));

      [Fact]
      public void FromConnectionString_WithBucketAndCredentials()
      {
         IBlobStorage module = Factory.FromConnectionString(
            "aws.s3://keyId=a;key=b;bucket=c;serviceUrl=https://s3.example.local"
         );

         Assert.IsAssignableFrom<IAwsS3BlobStorage>(module);
      }
   }
}
