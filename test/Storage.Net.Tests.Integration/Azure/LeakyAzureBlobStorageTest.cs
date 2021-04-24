using Azure.Storage;
using Azure.Storage.Blobs;
using Storage.Net.Blobs;
using Storage.Net.Microsoft.Azure.Storage.Blobs;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Xunit;
using AzureNative = global::Azure;

namespace Storage.Net.Tests.Integration.Azure
{
   [Trait("Category", "Blobs")]
   public class LeakyAzureBlobStorageTest
   {
      private readonly IAzureBlobStorage _service;
      private readonly BlobServiceClient _native;

      public LeakyAzureBlobStorageTest()
      {
         ITestSettings settings = Settings.Instance;

         IBlobStorage storage = StorageFactory.Blobs.AzureBlobStorageWithSharedKey(
            settings.AzureStorageName, settings.AzureStorageKey);
         _service = (IAzureBlobStorage)storage;

         _native = new BlobServiceClient(
           new Uri($"https://{Settings.Instance.AzureStorageName}.blob.core.windows.net/"),
           new StorageSharedKeyCredential(Settings.Instance.AzureStorageName, Settings.Instance.AzureStorageKey));
      }

      [Fact]
      public async Task Sas_Account()
      {
         var policy = new AccountSasPolicy(DateTime.UtcNow, TimeSpan.FromHours(1));
         policy.Permissions =
            AccountSasPermission.List |
            AccountSasPermission.Read |
            AccountSasPermission.Write;
         string sas = await _service.GetStorageSasAsync(policy);
         Assert.NotNull(sas);

         //check we can connect and list containers
         IBlobStorage sasInstance = StorageFactory.Blobs.AzureBlobStorageWithSas(sas);
         IReadOnlyCollection<Blob> containers = await sasInstance.ListAsync(StoragePath.RootFolderPath);
         Assert.True(containers.Count > 0);
      }

      [Fact]
      public async Task Sas_Container()
      {
         string fileName = Guid.NewGuid().ToString() + ".containersas.txt";
         string filePath = StoragePath.Combine("test", fileName);
         await _service.WriteTextAsync(filePath, "whack!");

         var policy = new ContainerSasPolicy(DateTime.UtcNow, TimeSpan.FromHours(1));
         string sas = await _service.GetContainerSasAsync("test", policy, true);

         //check we can connect and list test file in the root
         IBlobStorage sasInstance = StorageFactory.Blobs.AzureBlobStorageWithSas(sas);
         IReadOnlyCollection<Blob> blobs = await sasInstance.ListAsync();
         Blob testBlob = blobs.FirstOrDefault(b => b.Name == fileName);
         Assert.NotNull(testBlob);
      }

      [Theory]
      [InlineData("")]
      [InlineData("directory/")]
      public async Task Sas_Container_StoresBlob_NameDoesNotContainLeadingOrDoubleSlash(string directoryName)
      {
         string fileName = Guid.NewGuid().ToString() + ".containersas.txt";
         await _service.CreateFolderAsync("test");

         var policy = new ContainerSasPolicy(DateTime.UtcNow, TimeSpan.FromHours(1));
         policy.Permissions = ContainerSasPermission.Create;
         string sas = await _service.GetContainerSasAsync("test", policy, true);

         IBlobStorage sasInstance = StorageFactory.Blobs.AzureBlobStorageWithSas(sas);
         await sasInstance.WriteTextAsync(directoryName + fileName, "file content");

         AzureNative.Storage.Blobs.Models.BlobItem freshBlob = null;
         BlobContainerClient nativeContainerClient = _native.GetBlobContainerClient("test");
         await foreach(AzureNative.Storage.Blobs.Models.BlobItem blob in nativeContainerClient.GetBlobsAsync())
         {
            if(blob.Name.Contains(fileName))
            {
               freshBlob = blob;
               break;
            }
         }

         Assert.False(freshBlob.Name.StartsWith('/'));
         Assert.DoesNotContain("//", freshBlob.Name);
      }

      [Fact]
      public async Task ContainerPublicAccess()
      {
         //make sure container exists
         await _service.WriteTextAsync("test/one", "test");
         await _service.SetContainerPublicAccessAsync("test", ContainerPublicAccessType.Off);

         ContainerPublicAccessType pa = await _service.GetContainerPublicAccessAsync("test");
         Assert.Equal(ContainerPublicAccessType.Off, pa);   //it's off by default

         //set to public
         await _service.SetContainerPublicAccessAsync("test", ContainerPublicAccessType.Container);
         pa = await _service.GetContainerPublicAccessAsync("test");
         Assert.Equal(ContainerPublicAccessType.Container, pa);
      }

      [Fact]
      public async Task Sas_BlobPublicAccess()
      {
         string path = StoragePath.Combine("test", Guid.NewGuid().ToString() + ".txt");

         await _service.WriteTextAsync(path, "read me!");

         var policy = new BlobSasPolicy(DateTime.UtcNow, TimeSpan.FromHours(12))
         {
            Permissions = BlobSasPermission.Read | BlobSasPermission.Write
         };

         string publicUrl = await _service.GetBlobSasAsync(path);

         Assert.NotNull(publicUrl);

         string text = await new HttpClient().GetStringAsync(publicUrl);
         Assert.Equal("read me!", text);
      }

      [Fact]
      public async Task Lease_CanAcquireAndRelease()
      {
         string id = $"test/{nameof(Lease_CanAcquireAndRelease)}.lck";

         await _service.BreakLeaseAsync(id, true);

         using(AzureStorageLease lease = await _service.AcquireLeaseAsync(id, TimeSpan.FromSeconds(20)))
         {
            
         }
      }

      [Fact]
      public async Task Lease_Break()
      {
         string id = $"test/{nameof(Lease_Break)}.lck";

         await _service.BreakLeaseAsync(id, true);

         await _service.AcquireLeaseAsync(id, TimeSpan.FromSeconds(20));

         await _service.BreakLeaseAsync(id);
      }

      [Fact]
      public async Task Lease_FailsOnAcquiredLeasedBlob()
      {
         string id = $"test/{nameof(Lease_FailsOnAcquiredLeasedBlob)}.lck";

         await _service.BreakLeaseAsync(id, true);

         using(AzureStorageLease lease1 = await _service.AcquireLeaseAsync(id, TimeSpan.FromSeconds(20)))
         {
            await Assert.ThrowsAsync<StorageException>(() => _service.AcquireLeaseAsync(id, TimeSpan.FromSeconds(20)));
         }
      }

      [Fact]
      public async Task Lease_WaitsToReleaseAcquiredLease()
      {
         string id = $"test/{nameof(Lease_WaitsToReleaseAcquiredLease)}.lck";

         await _service.BreakLeaseAsync(id, true);

         using(AzureStorageLease lease1 = await _service.AcquireLeaseAsync(id, TimeSpan.FromSeconds(20)))
         {
            await _service.AcquireLeaseAsync(id, TimeSpan.FromSeconds(20), null, true);
         }
      }

      [Fact]
      public async Task Lease_Container_CanAcquireAndRelease()
      {
         string id = "test";

         await _service.BreakLeaseAsync(id, true);

         using(AzureStorageLease lease = await _service.AcquireLeaseAsync(id, TimeSpan.FromSeconds(15)))
         {

         }
      }

      [Fact]
      public async Task Lease_Container_Break()
      {
         string id = "test";

         await _service.BreakLeaseAsync(id, true);

         await _service.AcquireLeaseAsync(id, TimeSpan.FromSeconds(15));

         await _service.BreakLeaseAsync(id);
      }

      [Fact]
      public async Task Top_level_folders_are_containers()
      {
         IReadOnlyCollection<Blob> containers = await _service.ListAsync();

         foreach(Blob container in containers)
         {
            Assert.Equal(BlobItemKind.Folder, container.Kind);
            Assert.True(container.Properties?.ContainsKey("IsContainer"), "isContainer property not present at all");
            Assert.Equal(true, container.Properties["IsContainer"]);
         }
      }

      [Fact]
      public async Task Delete_container()
      {
         string containerName = Guid.NewGuid().ToString();
         await _service.WriteTextAsync($"{containerName}/test.txt", "test");

         IReadOnlyCollection<Blob> containers = await _service.ListAsync();
         Assert.Contains(containers, c => c.Name == containerName);

         await _service.DeleteAsync(containerName);
         containers = await _service.ListAsync();
         Assert.DoesNotContain(containers, c => c.Name == containerName);
      }

      /*[Fact]
      public async Task Analytics_has_logs_container()
      {
         IReadOnlyCollection<Blob> containers = await _native.ListAsync();
         Assert.Contains(containers, c => c.Name == "$logs");
      }*/

      /*[Fact]
      public async Task Snapshots_create()
      {
         string path = "test/test.txt";

         await _native.WriteTextAsync(path, "test");

         Blob snapshot = await _native.CreateSnapshotAsync(path);

         Assert.NotNull(snapshot);
      }*/
   }
}