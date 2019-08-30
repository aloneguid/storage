using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Storage.Net.Blobs;
using Storage.Net.Microsoft.Azure.DataLake.Store.Gen2;
using Storage.Net.Microsoft.Azure.DataLake.Store.Gen2.Model;
using Xunit;

namespace Storage.Net.Tests.Integration.Azure
{
   [Trait("Category", "Blobs")]
   public class LeakyAdlsGen2StorageTest : IAsyncLifetime
   {
      private readonly ITestSettings _settings;
      private readonly IAzureDataLakeGen2BlobStorage _storage;
      private const string Filesystem = "test";

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
         IBlobStorage authInstance =
            StorageFactory.Blobs.AzureDataLakeGen2StoreBySharedAccessKey(_settings.AzureDataLakeGen2Name,
               _settings.AzureDataLakeGen2Key);

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

      /*[Fact]
      public async Task Resolution_get_upn_from_objectId()
      {
         string upn = await _storage.AclObjectIdToUpnAsync(_settings.AzureDataLakeGen2TestObjectId);

         Assert.NotNull(upn);
      }*/

      [Fact]
      public async Task Acl_assign_permisssions_to_file_for_user()
      {
         string path = StoragePath.Combine(Filesystem, Guid.NewGuid().ToString());
         string userId = _settings.AzureDataLakeGen2TestObjectId;

         //write something
         await _storage.WriteTextAsync(path, "perm?");

         //check that user has no permissions
         AccessControl access = await _storage.GetAccessControlAsync(path);
         Assert.DoesNotContain(access.Acl, x => x.ObjectId == userId);

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

      [Fact]
      public async Task Acl_assign_non_default_permisssions_to_directory_for_user()
      {
         string directoryPath = StoragePath.Combine(Filesystem, "aclnondefault");
         string filePath = StoragePath.Combine(directoryPath, Guid.NewGuid().ToString());
         string userId = _settings.AzureDataLakeGen2TestObjectId;

         //write something
         await _storage.WriteTextAsync(filePath, "perm?");

         //check that user has no permissions
         AccessControl access = await _storage.GetAccessControlAsync(directoryPath);
         Assert.DoesNotContain(access.Acl, x => x.ObjectId == userId);

         //assign user a write permission
         access.Acl.Add(new AclEntry(ObjectType.User, userId, false, true, false));
         await _storage.SetAccessControlAsync(directoryPath, access);

         //check user has permissions now
         access = await _storage.GetAccessControlAsync(directoryPath);
         AclEntry userAcl = access.Acl.First(e => e.ObjectId == userId);
         Assert.False(userAcl.CanRead);
         Assert.True(userAcl.CanWrite);
         Assert.False(userAcl.CanExecute);
      }

      [Fact]
      public async Task Acl_assign_default_permisssions_to_directory_for_user()
      {
         string directoryPath = StoragePath.Combine(Filesystem, "acldefault");
         string filePath = StoragePath.Combine(directoryPath, Guid.NewGuid().ToString());
         string userId = _settings.AzureDataLakeGen2TestObjectId;

         //write something
         await _storage.WriteTextAsync(filePath, "perm?");

         //check that user has no permissions
         AccessControl access = await _storage.GetAccessControlAsync(directoryPath);
         Assert.DoesNotContain(access.Acl, x => x.ObjectId == userId);

         //assign user a write permission
         access.DefaultAcl.Add(new AclEntry(ObjectType.User, userId, false, true, false));
         await _storage.SetAccessControlAsync(directoryPath, access);

         //check user has permissions now
         access = await _storage.GetAccessControlAsync(directoryPath);
         AclEntry userAcl = access.DefaultAcl.First(e => e.ObjectId == userId);
         Assert.False(userAcl.CanRead);
         Assert.True(userAcl.CanWrite);
         Assert.False(userAcl.CanExecute);
      }

      [Fact]
      public async Task Creates_deletes_and_lists_a_filesystem()
      {
         const string filesystem = "filesystemtest";

         Assert.DoesNotContain(await _storage.ListFilesystemsAsync(), x => x == filesystem);

         await _storage.CreateFilesystemAsync(filesystem);
         Assert.Contains(await _storage.ListFilesystemsAsync(), x => x == filesystem);

         await _storage.DeleteFilesystemAsync(filesystem);
         Assert.DoesNotContain(await _storage.ListFilesystemsAsync(), x => x == filesystem);
      }

      public async Task InitializeAsync()
      {
         //drop all blobs in test storage
         IReadOnlyCollection<Blob> topLevel =
            (await _storage.ListAsync(recurse: false, folderPath: Filesystem)).ToList();

         try
         {
            await _storage.DeleteAsync(topLevel.Select(f => f.FullPath));
         }
         catch
         {
            //suppress exception to resume test attempt
         }
      }

      public Task DisposeAsync()
      {
         return Task.CompletedTask;
      }
   }
}
