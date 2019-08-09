using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Storage.Net.Blobs;
using Xunit;

namespace Storage.Net.Tests.Integration.Azure
{
   [Trait("Category", "Blobs")]
   public class LeakyAdlsGen2StorageTest
   {
      private readonly ITestSettings _settings;
      private readonly IBlobStorage _storage;

      public LeakyAdlsGen2StorageTest()
      {
         _settings = Settings.Instance;

         _storage = StorageFactory.Blobs.AzureDataLakeGen2StoreBySharedAccessKey(
            _settings.AzureDataLakeGen2Name,
            _settings.AzureDataLakeGen2Key);
      }

      [Fact]
      public async Task Authenticate_with_shared_key()
      {
         IBlobStorage authInstance = StorageFactory.Blobs.AzureDataLakeGen2StoreBySharedAccessKey(_settings.AzureDataLakeGen2Name, _settings.AzureDataLakeGen2Key);

         //trigger any operation
         await authInstance.ListAsync();
      }

      [Fact]
      public async Task Authenticate_with_service_principal()
      {
         IBlobStorage authInstance = StorageFactory.Blobs.AzureDataLakeGen2StoreByClientSecret(
            _settings.AzureDataLakeGen2Name,
            _settings.AzureDataLakeGen2TenantId,
            _settings.AzureDataLakeGen2PrincipalId,
            _settings.AzureDataLakeGen2PrincipalSecret);

         //trigger any operation
         await authInstance.ListAsync();
      }
   }
}
