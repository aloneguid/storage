using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Config.Net;
using NetBox;
using NetBox.Extensions;
using NetBox.Generator;
using Storage.Net.Blob;
using Xunit;

namespace Storage.Net.Tests.Integration.Blobs
{
    #region [ Test Variations ]

    public class AzureBlobStorageProviderTest : BlobStorageTest
   {
      public AzureBlobStorageProviderTest() : base("azure", "testcontainer/") { }
   }

   public class AzureDataLakeBlobStorageProviderTest : BlobStorageTest
   {
      public AzureDataLakeBlobStorageProviderTest() : base("azure-datalakestore") { }
   }

   public class DiskDirectoryBlobStorageProviderTest : BlobStorageTest
   {
      public DiskDirectoryBlobStorageProviderTest() : base("disk-directory") { }
   }

   public class AwsS3BlobStorageProviderTest : BlobStorageTest
   {
      public AwsS3BlobStorageProviderTest() : base("aws-s3") { }
   }

   public class InMemboryBlobStorageProviderTest : BlobStorageTest
   {
      public InMemboryBlobStorageProviderTest() : base("inmemory") { }
   }

   public class AzureKeyVaultBlobStorageProviderTest : BlobStorageTest
   {
      public AzureKeyVaultBlobStorageProviderTest() : base("azurekeyvault") { }
   }

   public class ZipFileBlobStorageProviderTest : BlobStorageTest
   {
      public ZipFileBlobStorageProviderTest() : base("zip") { }
   }

    //ftp i snot ready yet
    /*public class FtpBlobStorageProviderTest : BlobStorageTest
    {
        public FtpBlobStorageProviderTest() : base("ftp") { }
    }*/

   #endregion

   public abstract class BlobStorageTest : AbstractTestFixture, IAsyncLifetime
   {
      private readonly string _type;
      private readonly string _blobPrefix;
      private IBlobStorage _storage;
      private ITestSettings _settings;

        public BlobStorageTest(string type, string blobPrefix = null)
        {
            StorageFactory.Modules.UseAzureStorage();

            _settings = new ConfigurationBuilder<ITestSettings>()
               .UseIniFile("c:\\tmp\\integration-tests.ini")
               .UseEnvironmentVariables()
               .Build();

            _type = type;
            _blobPrefix = blobPrefix;
            switch (_type)
            {
                case "azure":
                    _storage = StorageFactory.Blobs.AzureBlobStorage(_settings.AzureStorageName, _settings.AzureStorageKey);
                    break;
                case "azure-datalakestore":
                    _storage = StorageFactory.Blobs.AzureDataLakeStoreByClientSecret(
                       _settings.AzureDataLakeStoreAccountName,
                       _settings.AzureDataLakeTenantId,
                       _settings.AzureDataLakePrincipalId,
                       _settings.AzureDataLakePrincipalSecret);
                    break;
                case "disk-directory":
                    _storage = StorageFactory.Blobs.DirectoryFiles(TestDir);
                    break;
                case "zip":
                    _storage = StorageFactory.Blobs.ZipFile(Path.Combine(TestDir.FullName, "test.zip"));
                    break;
                case "aws-s3":
                    _storage = StorageFactory.Blobs.AmazonS3BlobStorage(
                       _settings.AwsAccessKeyId,
                       _settings.AwsSecretAccessKey,
                       _settings.AwsTestBucketName);
                    break;
                case "inmemory":
                    _storage = StorageFactory.Blobs.InMemory();
                    break;
                case "azurekeyvault":
                    _storage = StorageFactory.Blobs.AzureKeyVault(
                       _settings.KeyVaultUri,
                       _settings.KeyVaultCreds);
                    break;
                case "ftp":
                    _storage = StorageFactory.Blobs.Ftp(_settings.FtpHostName, new NetworkCredential(_settings.FtpUsername, _settings.FtpPassword));
                    break;
            }
        }

      public Task InitializeAsync()
      {
            return Task.FromResult(true);
      }

      public async Task DisposeAsync()
      {
         try
         {
            IReadOnlyCollection<BlobId> allFiles = await _storage.ListFilesAsync(null);
            await _storage.DeleteAsync(allFiles.Select(id => id.FullPath));
         }
         catch
         {
            //just don't care
         }
      }

      private async Task<string> GetRandomStreamIdAsync(string prefix = null)
      {
         string id = RandomBlobId();
         if (prefix != null) id = prefix + "/" + id;

         using (Stream s = "kjhlkhlkhlkhlkh".ToMemoryStream())
         {
            await _storage.WriteAsync(id, s);
         }

         return id;
      }

      [Fact]
      public async Task List_All_DoesntCrash()
      {
         List<BlobId> allBlobNames = (await _storage.ListAsync(new ListOptions { Recurse = true })).ToList();
      }

      [Fact]
      public async Task List_RootFolder_HasAtLeastOne()
      {
         string targetId = RandomBlobId();

         await _storage.WriteTextAsync(targetId, "test");

         IReadOnlyCollection<BlobId> rootContent = await _storage.ListAsync(new ListOptions { Recurse = false, IncludeMetaWhenKnown = true });

         Assert.NotEmpty(rootContent);
      }

      [Fact]
      public async Task List_ByFilePrefix_Filtered()
      {
         string prefix = RandomGenerator.RandomString;

         int countBefore = (await _storage.ListAsync(new ListOptions { FolderPath = _blobPrefix, FilePrefix = prefix})).Count();

         string id1 = RandomBlobId(prefix);
         string id2 = RandomBlobId(prefix);
         string id3 = RandomBlobId();

         await _storage.WriteTextAsync(id1, RandomGenerator.RandomString);
         await _storage.WriteTextAsync(id2, RandomGenerator.RandomString);
         await _storage.WriteTextAsync(id3, RandomGenerator.RandomString);

         List<BlobId> items = (await _storage.ListAsync(new ListOptions { FolderPath = _blobPrefix, FilePrefix = prefix})).ToList();
         Assert.Equal(2 + countBefore, items.Count); //2 files + containing folder
      }

      [Fact]
      public async Task List_FilesInFolder_NonRecursive()
      {
         string id = RandomBlobId();

         await _storage.WriteTextAsync(id, RandomGenerator.RandomString);

         List<BlobId> items = (await _storage.ListAsync(new ListOptions { FolderPath = _blobPrefix, Recurse = false })).ToList();

         Assert.True(items.Count > 0);

         BlobId tid = items.Where(i => i.FullPath == id).FirstOrDefault();
         Assert.NotNull(tid);
      }

      [Fact]
      public async Task List_FilesInFolder_Recursive()
      {
         string id1 = RandomBlobId();
         string id2 = StoragePath.Combine(RandomBlobId(), RandomGenerator.RandomString);
         string id3 = StoragePath.Combine(RandomBlobId(), RandomGenerator.RandomString, RandomGenerator.RandomString);

         try
         {
            await _storage.WriteTextAsync(id1, RandomGenerator.RandomString);
            await _storage.WriteTextAsync(id2, RandomGenerator.RandomString);
            await _storage.WriteTextAsync(id3, RandomGenerator.RandomString);

            IEnumerable<BlobId> items = await _storage.ListAsync(new ListOptions { Recurse = true });
         }
         catch(NotSupportedException)
         {
            //it ok for providers not to support hierarchy
         }
      }

      [Fact]
      public async Task List_InNonExistingFolder_EmptyCollection()
      {
         IEnumerable<BlobId> objects = await _storage.ListAsync(new ListOptions { FolderPath = RandomBlobId() });

         Assert.NotNull(objects);
         Assert.True(objects.Count() == 0);
      }

      [Fact]
      public async Task List_FilesInNonExistingFolder_EmptyCollection()
      {
         IEnumerable<BlobId> objects = await _storage.ListFilesAsync(new ListOptions { FolderPath = RandomBlobId() });

         Assert.NotNull(objects);
         Assert.True(objects.Count() == 0);
      }

      [Fact]
      public async Task List_VeryLongPrefix_NoResultsNoCrash()
      {
         await Assert.ThrowsAsync<ArgumentException>(async () => await _storage.ListAsync(new ListOptions { FilePrefix = RandomGenerator.GetRandomString(100000, false) }));
      }

      [Fact]
      public async Task List_limited_number_of_results()
      {
         string prefix = RandomGenerator.RandomString;
         string id1 = RandomBlobId(prefix);
         string id2 = RandomBlobId(prefix);
         await _storage.WriteTextAsync(id1, RandomGenerator.RandomString);
         await _storage.WriteTextAsync(id2, RandomGenerator.RandomString);

         int countAll = (await _storage.ListFilesAsync(new ListOptions { FolderPath = _blobPrefix, FilePrefix = prefix })).Count();
         int countOne = (await _storage.ListAsync(new ListOptions { FolderPath = _blobPrefix, FilePrefix = prefix, MaxResults = 1 })).Count();

         Assert.Equal(2, countAll);
         Assert.Equal(1, countOne);
      }

      [Fact]
      public async Task List_with_browsefilter_calls_filter()
      {
         string id1 = RandomBlobId();
         string id2 = RandomBlobId();
         await _storage.WriteTextAsync(id1, RandomGenerator.RandomString);
         await _storage.WriteTextAsync(id2, RandomGenerator.RandomString);

         //dump compare
         IReadOnlyCollection<BlobId> files = await _storage.ListFilesAsync(new ListOptions
         {
            Recurse = true
         });
         Assert.Contains(files, f => f.FullPath == id1 && f.Kind == BlobItemKind.File);

         //server-side filtering
         files = await _storage.ListFilesAsync(new ListOptions
         {
            Recurse = true,
            BrowseFilter = id => (id.Kind != BlobItemKind.File || id.FullPath == id1)
         });


         Assert.Equal(1, files.Count);
         Assert.Equal(id1, files.First().FullPath);
      }

      [Fact]
      public async Task GetMeta_for_one_file_succeeds()
      {
         string content = RandomGenerator.GetRandomString(1000, false);
         string id = RandomBlobId();

         await _storage.WriteTextAsync(id, content);

         BlobMeta meta = await _storage.GetMetaAsync(id);


         long size = Encoding.UTF8.GetBytes(content).Length;
         string md5 = content.GetHash(HashType.Md5);

         Assert.Equal(size, meta.Size);
         if (meta.MD5 != null) Assert.Equal(md5, meta.MD5);
         if (meta.LastModificationTime != null) Assert.Equal(DateTime.UtcNow.RoundToDay(), meta.LastModificationTime.Value.DateTime.RoundToDay());
      }

      [Fact]
      public async Task GetMeta_doesnt_exist_returns_null()
      {
         string id = RandomBlobId();

         BlobMeta meta = (await _storage.GetMetaAsync(new[] { id })).First();

         Assert.Null(meta);
      }

      [Fact]
      public async Task Open_doesnt_exist_returns_null()
      {
         string id = RandomBlobId();

         Assert.Null(await _storage.OpenReadAsync(id));
      }

      [Fact]
      public async Task Open_copy_to_memory_stream_succeeds()
      {
         string id = await GetRandomStreamIdAsync();
         IBlobStorage ms = StorageFactory.Blobs.InMemory();

         //if this doesn't crash it means the returned stream is compatible with usual .net streaming
         await _storage.CopyToAsync(id, ms, id);
      }

      [Fact]
      public async Task Write_with_openwrite_succeeds()
      {
         string id = RandomBlobId();
         byte[] data = Encoding.UTF8.GetBytes("oh my");

         using (Stream dest = await _storage.OpenWriteAsync(id))
         {
            await dest.WriteAsync(data, 0, data.Length);
         }

         //read and check
         string result = await _storage.ReadTextAsync(id);
         Assert.Equal("oh my", result);
      }

      [Fact]
      public async Task Exists_non_existing_blob_returns_false()
      {
         Assert.False(await _storage.ExistsAsync(RandomBlobId()));
      }

      [Fact]
      public async Task Exists_existing_blob_returns_true()
      {
         string id = RandomBlobId();
         await _storage.WriteTextAsync(id, "test");

         Assert.True(await _storage.ExistsAsync(id));
      }

      [Fact]
      public async Task Delete_create_and_delete_doesnt_exist()
      {
         string id = RandomBlobId();
         await _storage.WriteTextAsync(id, "test");
         await _storage.DeleteAsync(id);

         Assert.False(await _storage.ExistsAsync(id));

      }

      private string RandomBlobId(string prefix = null)
      {
         return _blobPrefix +
            (prefix == null ? string.Empty : prefix) +
            Guid.NewGuid().ToString();
      }

      class TestDocument
      {
         public string M { get; set; }
      }

      [Fact]
      public void Dispose_does_not_fail()
      {
         _storage.Dispose();
      }
   }
}
