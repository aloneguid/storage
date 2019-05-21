using Storage.Net.Blob;
using Storage.Net.Microsoft.Azure.Storage.Blob;
using System;
using System.Net;
using System.Threading.Tasks;
using Xunit;

namespace Storage.Net.Tests.Integration.Azure
{
   public class LeakyAzureBlobStorageTest
   {
      private readonly IBlobStorage _storage;
      private readonly IAzureBlobStorage _native;

      public LeakyAzureBlobStorageTest()
      {
         ITestSettings settings = Settings.Instance;

         _storage = StorageFactory.Blobs.AzureBlobStorage(settings.AzureStorageName, settings.AzureStorageKey);
         _native = (IAzureBlobStorage)_storage;
      }

      [Fact]
      public async Task GetReadOnlySasUriAsync()
      {
         string id = "test/single.txt";
         await _storage.WriteTextAsync(id, "test");

         string uri = await _native.GetReadOnlySasUriAsync(id);
         string content = await new WebClient().DownloadStringTaskAsync(new Uri(uri));
         Assert.Equal("test", content);
      }

      [Fact]
      public async Task CanAcquireAndReleaseLeaseAsync()
      {
         string id = $"test/{nameof(CanAcquireAndReleaseLeaseAsync)}.lck";

         using(BlobLease lease = await _native.AcquireBlobLeaseAsync(id, TimeSpan.FromSeconds(20)))
         {
            
         }
      }

      [Fact]
      public async Task FailsOnAcquiredLock()
      {
         string id = $"test/{nameof(FailsOnAcquiredLock)}.lck";

         using(BlobLease lease1 = await _native.AcquireBlobLeaseAsync(id, TimeSpan.FromSeconds(20)))
         {
            await Assert.ThrowsAsync<StorageException>(() => _native.AcquireBlobLeaseAsync(id, TimeSpan.FromSeconds(20)));
         }
      }

      [Fact]
      public async Task WaitsToReleaseAcquiredLock()
      {
         string id = $"test/{nameof(WaitsToReleaseAcquiredLock)}.lck";

         using(BlobLease lease1 = await _native.AcquireBlobLeaseAsync(id, TimeSpan.FromSeconds(20)))
         {
            await _native.AcquireBlobLeaseAsync(id, TimeSpan.FromSeconds(20), true);
         }
      }
   }
}