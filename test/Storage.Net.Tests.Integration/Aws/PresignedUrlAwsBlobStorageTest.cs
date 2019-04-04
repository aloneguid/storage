using System;
using System.IO;
using System.Net;
using System.Threading.Tasks;
using Amazon;
using Amazon.S3;
using NetBox.Extensions;
using Shouldly;
using Storage.Net.Amazon.Aws.Blob;
using Storage.Net.Blob;
using Storage.Net.Model;
using Storage.Net.Tests.Integration.Utils;
using Xunit;
using Xunit.Abstractions;

namespace Storage.Net.Tests.Integration.Aws
{
   public class PresignedUrlAwsBlobStorageTest : IDisposable
   {
      private readonly ITestOutputHelper _output;
      private readonly IBlobStorage _blobStorage;
      private readonly IAwsS3BlobStorageNativeOperations _awsClient;
      private const string OctMimeType = MimeType.ApplicationOctetStream;

      private const string TestFileName = @"test.bin";
      private const string TestFilePath = @"Data\" + TestFileName;

      private const string AccessKey = "Q3AM3UQ867SPQQA43P2F";
      private const string SecretKey = "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG";

      private const string ServiceUrl = "https://play.minio.io:9000";

      private const string TestBucket = "storage-test-bucket";

      public PresignedUrlAwsBlobStorageTest(ITestOutputHelper output)
      {
         Utility.PrepareTestData(TestFilePath);

         _output = output;

         _blobStorage = StorageFactory.Blobs
             .AmazonS3BlobStorage(AccessKey, SecretKey, TestBucket, new AmazonS3Config()
             {
                RegionEndpoint = RegionEndpoint.USEast1,
                ServiceURL = ServiceUrl,
                ForcePathStyle = true
             });
         _awsClient = ((IAwsS3BlobStorageNativeOperations)_blobStorage);
      }

      [Fact]
      public async Task Should_Get_PreSigned_Upload_Url()
      {
         // Arrange

         await EnsureExists();

         string bucket = TestBucket;
         string id = nameof(Should_Get_PreSigned_Upload_Url);
         string contain = $"{bucket}/{id}";

         string uploadUrl = null;

         // Act

         uploadUrl = await _awsClient.GetUploadUrlAsync(id, OctMimeType);

         // Assert

         uploadUrl.ShouldContain(contain);
      }

      [Fact]
      public async Task Should_Upload_File_By_PreSigned_Url_Then_Check_Size()
      {
         // Arrange

         await EnsureExists();

         string id = nameof(Should_Upload_File_By_PreSigned_Url_Then_Check_Size);

         long srcSize = 0;
         long dstSize = -1;

         string uploadUrl = await _awsClient.GetUploadUrlAsync(id, OctMimeType);

         // Act

         using(FileStream fs = File.OpenRead(TestFileName))
         {
            srcSize = fs.Length;

            using(var client = new WebClient())
            {
               client.Headers.Add("Content-Type", OctMimeType);
               await client.UploadDataTaskAsync(uploadUrl, HttpVerb.PUT.ToString(), fs.ToByteArray());
            }
         }

         BlobMeta metadata = await _blobStorage.GetMetaAsync(id);

         if(metadata != null)
            dstSize = metadata.Size;

         // Assert

         dstSize.ShouldBe(srcSize);
      }

      [Fact]
      public async Task Should_Get_PreSigned_Download_Url()
      {
         // Arrange

         await EnsureExists();

         string bucket = TestBucket;
         string id = nameof(Should_Get_PreSigned_Download_Url);
         string contain = $"{bucket}/{id}";

         string downloadUrl = null;

         await UploadFileAsync(id, TestFileName);

         // Act

         downloadUrl = await _awsClient.GetDownloadUrlAsync(id, OctMimeType);

         // Assert

         downloadUrl.ShouldContain(contain);
      }

      [Fact]
      public async Task Should_Download_File_By_PreSigned_Url_Then_Check_Size()
      {
         // Arrange

         await EnsureExists();

         string id = nameof(Should_Download_File_By_PreSigned_Url_Then_Check_Size);

         long srcSize = await UploadFileAsync(id, TestFileName);
         long dstSize = -1;

         string downloadUrl = await _awsClient.GetDownloadUrlAsync(id, OctMimeType);

         _output.WriteLine(downloadUrl);

         // Act

         using(var client = new WebClient())
         {
            client.Headers.Add("Content-Type", OctMimeType);
            byte[] result = await client.DownloadDataTaskAsync(downloadUrl);
            dstSize = result.LongLength;
         }

         // Assert

         dstSize.ShouldBe(srcSize);
      }

      [Fact]
      public async Task Should_Upload_File_Then_Check_Size()
      {
         // Arrange

         await EnsureExists();

         string id = nameof(Should_Upload_File_Then_Check_Size);

         long srcSize = 0;
         long dstSize = -1;

         // Act

         srcSize = await UploadFileAsync(id, TestFileName);

         BlobMeta metadata = await _blobStorage.GetMetaAsync(id);
         if(metadata != null)
            dstSize = metadata.Size;

         // Assert

         dstSize.ShouldBe(srcSize);
      }

      [Fact]
      public async Task Should_Upload_File_Then_Download_And_Check_Size()
      {
         // Arrange

         await EnsureExists();

         string id = nameof(Should_Upload_File_Then_Download_And_Check_Size);

         long srcSize = 0;
         long dstSize = -1;

         // Act

         srcSize = await UploadFileAsync(id, TestFileName);
         dstSize = await DownloadFileAsync(id);

         // Assert

         dstSize.ShouldBe(srcSize);
      }

      private async Task<long> UploadFileAsync(string id, string path)
      {
         using(FileStream fs = File.OpenRead(path))
         {
            long length = fs.Length;
            await _blobStorage.WriteAsync(id, fs);
            return length;
         }
      }

      private async Task<long> DownloadFileAsync(string id)
      {
         using(var ms = new MemoryStream())
         {
            await _blobStorage.ReadToStreamAsync(id, ms);
            return ms.Length;
         }
      }

      private async Task EnsureExists()
      {
         await _awsClient.NativeBlobClient.EnsureBucketExistsAsync(TestBucket);
      }

      /// <summary>Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.</summary>
      public void Dispose()
      {
         if(_awsClient.NativeBlobClient != null)
         {
            if(_awsClient.NativeBlobClient.DoesS3BucketExistAsync(TestBucket).Result)
            {
               foreach(BlobId file in _blobStorage.ListAsync().Result)
               {
                  _blobStorage.DeleteAsync(file.Id).Wait();
               }

               _awsClient.NativeBlobClient.DeleteBucketAsync(TestBucket).Wait();
            }
         }

         _blobStorage?.Dispose();

         Utility.ClearTestData(TestFilePath);
      }
   }
}