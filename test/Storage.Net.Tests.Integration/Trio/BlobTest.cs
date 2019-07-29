using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NetBox;
using NetBox.Extensions;
using NetBox.Generator;
using Storage.Net.Blobs;
using Xunit;

namespace Storage.Net.Tests.Integration.Blobs
{
   [Trait("Category", "Blobs")]
   public abstract class BlobTest : IAsyncLifetime
   {
      private readonly IBlobStorage _storage;
      private readonly string _blobPrefix;
      private readonly BlobFixture _fixture;

      public BlobTest(BlobFixture fixture)
      {
         _storage = fixture.Storage;
         _blobPrefix = fixture.BlobPrefix;
         _fixture = fixture;
      }

      public Task InitializeAsync()
      {
         return _fixture.InitAsync();  
      }

      public Task DisposeAsync()
      {
         return _fixture.DisposeAsync();
      }

      private async Task<string> GetRandomStreamIdAsync(string prefix = null)
      {
         string id = RandomBlobPath();
         if(prefix != null)
            id = prefix + "/" + id;

         using(Stream s = "kjhlkhlkhlkhlkh".ToMemoryStream())
         {
            await _storage.WriteAsync(id, s);
         }

         return id;
      }

      [Fact]
      public async Task List_All_DoesntCrash()
      {
         await _storage.ListAsync(new ListOptions { Recurse = true });
      }

      [Fact]
      public async Task List_RootFolder_HasAtLeastOne()
      {
         string targetId = RandomBlobPath();

         await _storage.WriteTextAsync(targetId, "test");

         IReadOnlyCollection<Blob> rootContent = await _storage.ListAsync();

         Assert.NotEmpty(rootContent);
      }

      [Fact]
      public async Task List_ByFilePrefix_Filtered()
      {
         string prefix = RandomGenerator.RandomString;

         int countBefore = (await _storage.ListAsync(new ListOptions { FolderPath = _blobPrefix, FilePrefix = prefix })).Count;

         string id1 = RandomBlobPath(prefix);
         string id2 = RandomBlobPath(prefix);
         string id3 = RandomBlobPath();

         await _storage.WriteTextAsync(id1, RandomGenerator.RandomString);
         await _storage.WriteTextAsync(id2, RandomGenerator.RandomString);
         await _storage.WriteTextAsync(id3, RandomGenerator.RandomString);

         IReadOnlyCollection<Blob> items = (await _storage.ListAsync(new ListOptions { FolderPath = _blobPrefix, FilePrefix = prefix }));
         Assert.Equal(2 + countBefore, items.Count); //2 files + containing folder
      }

      [Fact]
      public async Task List_FilesInFolder_NonRecursive()
      {
         string id = RandomBlobPath();

         await _storage.WriteTextAsync(id, RandomGenerator.RandomString);

         List<Blob> items = (await _storage.ListAsync(new ListOptions { FolderPath = _blobPrefix, Recurse = false })).ToList();

         Assert.True(items.Count > 0);

         Blob tid = items.FirstOrDefault(i => i.FullPath == id);
         Assert.NotNull(tid);
      }

      [Fact]
      public async Task List_FilesInFolder_Recursive()
      {
         string folderPath = RandomBlobPath();
         string id1 = StoragePath.Combine(folderPath, "1.txt");
         string id2 = StoragePath.Combine(folderPath, "sub", "2.txt");
         string id3 = StoragePath.Combine(folderPath, "sub", "3.txt");

         try
         {
            await _storage.WriteTextAsync(id1, RandomGenerator.RandomString);
            await _storage.WriteTextAsync(id2, RandomGenerator.RandomString);
            await _storage.WriteTextAsync(id3, RandomGenerator.RandomString);

            IReadOnlyCollection<Blob> items = await _storage.ListAsync(recurse: true, folderPath: folderPath);
            Assert.Equal(4, items.Count); //1.txt + sub (folder) + 2.txt + 3.txt

         }
         catch(NotSupportedException)
         {
            //it ok for providers not to support hierarchy
         }
      }

      [Fact]
      public async Task List_InNonExistingFolder_EmptyCollection()
      {
         IEnumerable<Blob> objects = await _storage.ListAsync(new ListOptions { FolderPath = RandomBlobPath() });

         Assert.NotNull(objects);
         Assert.True(objects.Count() == 0);
      }

      [Fact]
      public async Task List_FilesInNonExistingFolder_EmptyCollection()
      {
         IEnumerable<Blob> objects = await _storage.ListFilesAsync(new ListOptions { FolderPath = RandomBlobPath() });

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
         string id1 = RandomBlobPath(prefix);
         string id2 = RandomBlobPath(prefix);
         await _storage.WriteTextAsync(id1, RandomGenerator.RandomString);
         await _storage.WriteTextAsync(id2, RandomGenerator.RandomString);

         int countAll = (await _storage.ListFilesAsync(new ListOptions { FolderPath = _blobPrefix, FilePrefix = prefix })).Count;
         int countOne = (await _storage.ListAsync(new ListOptions { FolderPath = _blobPrefix, FilePrefix = prefix, MaxResults = 1 })).Count;

         Assert.Equal(2, countAll);
         Assert.Equal(1, countOne);
      }

      [Fact]
      public async Task List_with_browsefilter_calls_filter()
      {
         string id1 = RandomBlobPath();
         string id2 = RandomBlobPath();
         await _storage.WriteTextAsync(id1, RandomGenerator.RandomString);
         await _storage.WriteTextAsync(id2, RandomGenerator.RandomString);

         //dump compare
         IReadOnlyCollection<Blob> files = await _storage.ListFilesAsync(new ListOptions
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
      public async Task List_large_number_of_results()
      {
         const int count = 500;
         //arrange

         //something like FTP doesn't support multiple connections, however this should be implemented in FTP provider itself

         for(int it = 0; it < 50; it++)
         {
            await Task.WhenAll(Enumerable.Range(0, 10).Select(i => _storage.WriteTextAsync(RandomBlobPath(), "123")));
         }

         //act
         IReadOnlyCollection<Blob> blobs = await _storage.ListAsync(folderPath: _blobPrefix);

         //assert
         Assert.True(blobs.Count >= count, $"expected over {count}, but received only {blobs.Count}");
      }

      [Fact]
      public async Task List_folder_nonrecursively_no_children()
      {
         try
         {
            await _storage.WriteTextAsync("/sub/one.txt", "test");
            await _storage.WriteTextAsync("/sub/sub/two.txt", "test");

            IReadOnlyCollection<Blob> subItems = await _storage.ListAsync(recurse: false, folderPath: "sub");
            Assert.Equal(2, subItems.Count);


            Assert.Contains(new Blob("/sub/one.txt"), subItems);
            Assert.Contains(new Blob("/sub/sub", BlobItemKind.Folder), subItems);
         }
         catch(NotSupportedException)
         {
            //hierarchy not supported
         }
      }

      [Fact]
      public async Task GetBlob_for_one_file_succeeds()
      {
         string content = RandomGenerator.GetRandomString(1000, false);
         string id = RandomBlobPath();

         await _storage.WriteTextAsync(id, content);

         Blob meta = await _storage.GetBlobAsync(id);

         long size = Encoding.UTF8.GetBytes(content).Length;
         string md5 = content.GetHash(HashType.Md5);

         Assert.Equal(size, meta.Size);
         if(meta.MD5 != null)
            Assert.Equal(md5, meta.MD5);
         if(meta.LastModificationTime != null)
            Assert.Equal(DateTime.UtcNow.RoundToDay(), meta.LastModificationTime.Value.DateTime.RoundToDay());
      }

      [Fact]
      public async Task GetBlob_doesnt_exist_returns_null()
      {
         string id = RandomBlobPath();

         Blob meta = (await _storage.GetBlobsAsync(new[] { id })).First();

         Assert.Null(meta);
      }

      [Fact]
      public async Task Open_doesnt_exist_returns_null()
      {
         string id = RandomBlobPath();

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
         string id = RandomBlobPath();
         byte[] data = Encoding.UTF8.GetBytes("oh my");

         using(Stream dest = await _storage.OpenWriteAsync(id))
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
         Assert.False(await _storage.ExistsAsync(RandomBlobPath()));
      }

      [Fact]
      public async Task Exists_existing_blob_returns_true()
      {
         string id = RandomBlobPath();
         await _storage.WriteTextAsync(id, "test");

         Assert.True(await _storage.ExistsAsync(id));
      }

      [Fact]
      public async Task Delete_create_and_delete_doesnt_exist()
      {
         string path = RandomBlobPath();
         await _storage.WriteTextAsync(path, "test");
         await _storage.DeleteAsync(path);

         Assert.False(await _storage.ExistsAsync(path));
      }

      [Fact]
      public async Task Delete_folder_removes_all_files()
      {
         //setup
         string prefix = RandomBlobPath();
         string file1 = StoragePath.Combine(prefix, "1.txt");
         string file2 = StoragePath.Combine(prefix, "2.txt");


         try
         {
            //setup
            await _storage.WriteTextAsync(file1, "1");
            await _storage.WriteTextAsync(file2, "2");

            //act
            await _storage.DeleteAsync(prefix);
         }
         catch(NotSupportedException)
         {

         }

         //assert
         Assert.False(await _storage.ExistsAsync(file1));
         Assert.False(await _storage.ExistsAsync(file2));
      }

      [Fact]
      public async Task UserMetadata_write_readsback()
      {
         var blob = new Blob(RandomBlobPath());
         blob.Metadata["user"] = "ivan";
         blob.Metadata["fun"] = "no";

         await _storage.WriteTextAsync(blob, "test");
         try
         {
            await _storage.SetBlobAsync(blob);
         }
         catch(NotSupportedException)
         {
            return;
         }

         //test
         Blob blob2 = await _storage.GetBlobAsync(blob);
         Assert.NotNull(blob2.Metadata);
         Assert.Equal("ivan", blob2.Metadata["user"]);
         Assert.Equal("no", blob2.Metadata["fun"]);
         Assert.Equal(2, blob2.Metadata.Count);
      }

      [Fact]
      public async Task UserMetadata_OverwriteWithLess_RemovesOld()
      {
         //setup
         var blob = new Blob(RandomBlobPath());
         blob.Metadata["user"] = "ivan";
         blob.Metadata["fun"] = "no";
         await _storage.WriteTextAsync(blob, "test");
         try
         {
            await _storage.SetBlobAsync(blob);
         }
         catch(NotSupportedException)
         {
            return;
         }
         blob.Metadata.Clear();
         blob.Metadata["user"] = "ivan2";
         await _storage.WriteTextAsync(blob, "test2");
         await _storage.SetBlobAsync(blob);

         //test
         Blob blob2 = await _storage.GetBlobAsync(blob);
         Assert.NotNull(blob2.Metadata);
         Assert.Single(blob2.Metadata);
         Assert.Equal("ivan2", blob2.Metadata["user"]);
      }

      [Fact]
      public async Task UserMetadata_openwrite_readsback()
      {
         var blob = new Blob(RandomBlobPath());
         blob.Metadata["user"] = "ivan";
         blob.Metadata["fun"] = "no";

         using(Stream s = await _storage.OpenWriteAsync(blob))
         {
            s.Write(RandomGenerator.GetRandomBytes(10, 15));
         }
         try
         {
            await _storage.SetBlobAsync(blob);
         }
         catch(NotSupportedException)
         {
            return;
         }

         //test
         Blob blob2 = await _storage.GetBlobAsync(blob);
         Assert.NotNull(blob2.Metadata);
         Assert.Equal("ivan", blob2.Metadata["user"]);
         Assert.Equal("no", blob2.Metadata["fun"]);
         Assert.Equal(2, blob2.Metadata.Count);
      }

      [Fact]
      public async Task UserMetadata_List_AlsoReturnsMetadata()
      {
         var blob = new Blob(RandomBlobPath());
         blob.Metadata["user"] = "ivan";
         blob.Metadata["fun"] = "no";
         await _storage.WriteTextAsync(blob, "test2");

         try
         {
            await _storage.SetBlobAsync(blob);
         }
         catch(NotSupportedException)
         {
            return;
         }

         IReadOnlyCollection<Blob> all = await _storage.ListAsync(folderPath: blob.FolderPath, includeAttributes: true);

         //test
         Blob blob2 = all.First(b => b.FullPath == blob.FullPath);
         Assert.NotNull(blob2.Metadata);
         Assert.Equal("ivan", blob2.Metadata["user"]);
         Assert.Equal("no", blob2.Metadata["fun"]);
         Assert.Equal(2, blob2.Metadata.Count);
      }

      [Fact]
      public async Task GetMd5HashAsync()
      {
         var blob = new Blob(RandomBlobPath());
         string content = RandomGenerator.RandomString;
         string hash = content.GetHash(HashType.Md5);

         await _storage.WriteTextAsync(blob, content);

         string hash2 = await _storage.GetMD5HashAsync(blob);
         Assert.Equal(hash, hash2);
      }

      private string RandomBlobPath(string prefix = null)
      {
         return _blobPrefix +
            (prefix == null ? string.Empty : prefix) +
            Guid.NewGuid().ToString();
      }

      class TestDocument
      {
         public string M { get; set; }
      }
   }
}
