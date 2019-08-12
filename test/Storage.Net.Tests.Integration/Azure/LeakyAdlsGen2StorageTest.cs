using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Storage.Net.Blobs;
using Storage.Net.Microsoft.Azure.DataLake.Store.Gen2;
using Storage.Net.Microsoft.Azure.DataLake.Store.Gen2.Model;
using Xunit;

namespace Storage.Net.Tests.Integration.Azure
{
   [Trait("Category", "Blobs")]
   public class LeakyAdlsGen2StorageTest
   {
      private readonly ITestSettings _settings;
      private readonly IAzureDataLakeGen2BlobStorage _storage;

      public LeakyAdlsGen2StorageTest()
      {
         _settings = Settings.Instance;

         _storage = (IAzureDataLakeGen2BlobStorage)StorageFactory.Blobs.AzureDataLakeGen2StoreByClientSecret(
            _settings.AzureDataLakeGen2Name,
            _settings.AzureDataLakeGen2TenantId,
            _settings.AzureDataLakeGen2PrincipalId,
            _settings.AzureDataLakeGen2PrincipalSecret);
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

      [Fact]
      public async Task Acl_assign_permisssions_to_file_for_user()
      {
         string path = StoragePath.Combine("test", Guid.NewGuid().ToString());
         string userId = _settings.AzureDataLakeGen2TestObjectId;

         //write something
         await _storage.WriteTextAsync(path, "perm?");

         //check that user has no permissions
         AccessControl access = await _storage.GetAccessControlAsync(path);
         Assert.True(!access.Acl.Any(e => e.ObjectId == userId));

         //assign user a write permission
         access.Acl.Add(new AclEntry(ObjectType.User, userId, false, true, false));
         await _storage.SetAccessControlAsync(path, access);

         //check user has permissions now
         access = await _storage.GetAccessControlAsync(path);
         AclEntry userAcl = access.Acl.First(e => e.ObjectId == userId);
         Assert.False(userAcl.CanRead);
         Assert.True(userAcl.CanWrite);
         Assert.False(userAcl.CanExecute);
      }
   }
}
