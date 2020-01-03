using System;
using System.Threading.Tasks;
using Storage.Net.Blobs;
using Storage.Net.Microsoft.Azure.DataLake.Store.Gen1;
using Xunit;

namespace Storage.Net.Tests.Integration.Azure
{
   [Trait("Category", "Blobs")]
   public class LeakyAdlsGen1StorageTest
   {
      private readonly ITestSettings _settings;
      private readonly IAzureDataLakeGen1BlobStorage _storage;

      public LeakyAdlsGen1StorageTest()
      {
         _settings = Settings.Instance;

         _storage = (IAzureDataLakeGen1BlobStorage)StorageFactory.Blobs.AzureDataLakeGen1StoreByClientSecret(
            _settings.AzureGen1StorageName,
            _settings.TenantId,
            _settings.ClientId,
            _settings.ClientSecret);
      }

      [Fact]
      public async Task Acl_smoke()
      {
         string path = StoragePath.Combine(Guid.NewGuid().ToString(), Guid.NewGuid().ToString());
         await _storage.WriteTextAsync(path, "permgen1?");

         await _storage.GetAccessControlAsync(path);
      }

      /*[Fact]
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
      }*/
   }
}
