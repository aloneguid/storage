using Storage.Net.Blobs;
using Storage.Net.Microsoft.Azure.Storage.Blobs;
using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Xunit;

namespace Storage.Net.Tests.Integration.Azure
{
   [Trait("Category", "Other")]
   public class LeakyAzureBlobStorageTest
   {
      private readonly IAzureBlobStorage _native;

      public LeakyAzureBlobStorageTest()
      {
         ITestSettings settings = Settings.Instance;

         IBlobStorage storage = StorageFactory.Blobs.AzureBlobStorage(settings.AzureStorageName, settings.AzureStorageKey);
         _native = (IAzureBlobStorage)storage;
      }

      [Fact]
      public async Task GetReadOnlySasUriAsync()
      {
         string id = "test/single.txt";
         await _native.WriteTextAsync(id, "test");

         string uri = await _native.GetReadOnlySasUriAsync(id);
         string content = await new WebClient().DownloadStringTaskAsync(new Uri(uri));
         Assert.Equal("test", content);
      }

      [Fact]
      public async Task Lease_CanAcquireAndRelease()
      {
         string id = $"test/{nameof(Lease_CanAcquireAndRelease)}.lck";

         using(BlobLease lease = await _native.AcquireBlobLeaseAsync(id, TimeSpan.FromSeconds(20)))
         {
            
         }
      }

      [Fact]
      public async Task Lease_FailsOnAcquiredLeasedBlob()
      {
         string id = $"test/{nameof(Lease_FailsOnAcquiredLeasedBlob)}.lck";

         using(BlobLease lease1 = await _native.AcquireBlobLeaseAsync(id, TimeSpan.FromSeconds(20)))
         {
            await Assert.ThrowsAsync<StorageException>(() => _native.AcquireBlobLeaseAsync(id, TimeSpan.FromSeconds(20)));
         }
      }

      [Fact]
      public async Task Lease_WaitsToReleaseAcquiredLease()
      {
         string id = $"test/{nameof(Lease_WaitsToReleaseAcquiredLease)}.lck";

         using(BlobLease lease1 = await _native.AcquireBlobLeaseAsync(id, TimeSpan.FromSeconds(20)))
         {
            await _native.AcquireBlobLeaseAsync(id, TimeSpan.FromSeconds(20), true);
         }
      }

      [Fact]
      public async Task Top_level_folders_are_containers()
      {
         IReadOnlyCollection<Blob> containers = await _native.ListAsync();

         foreach(Blob container in containers)
         {
            Assert.Equal(BlobItemKind.Folder, container.Kind);
            Assert.True(container.Properties?.ContainsKey("IsContainer"), "isContainer property not present at all");
            Assert.Equal("True", container.Properties["IsContainer"]);
         }
      }
   }
}