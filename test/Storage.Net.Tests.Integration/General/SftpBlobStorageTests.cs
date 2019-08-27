using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Renci.SshNet.Sftp;
using Storage.Net.Blobs;
using Storage.Net.SFtp;
using Xunit;

namespace Storage.Net.Tests.Integration.General
{
   public class SftpBlobStorageTests
   {
      private IBlobStorage GetBlobStorage()
      {
         StorageFactory.Modules.UseSFtpStorage();
         return StorageFactory.Blobs.FromConnectionString("sftp://host=localhost;user=sftpuser;password=sftppassword;port=22");
      }

      [Fact]
      public async void ListRootFolderContents()
      {
         //var storage = StorageFactory.Blobs.SFtp("localhost", "sftpuser", "sftppassword");
         var storage = GetBlobStorage();
         var contentList = await storage.ListAsync(new ListOptions { FolderPath = "/" });
         Assert.NotEmpty(contentList);
      }

      [Fact]
      public void CreateFromConnectionString()
      {
         StorageFactory.Modules.UseSFtpStorage();
         var storage = StorageFactory.Blobs.FromConnectionString("sftp://host=localhost;user=sftpuser;password=sftppassword;port=22");
         Assert.IsType<SshNetSFtpBlobStorage>(storage);
      }

      [Fact]
      public async void OpenReadTest()
      {
         var storage = GetBlobStorage();
         var contentList = await storage.ListAsync(new ListOptions { FolderPath = "/in/" });
         var bobFile = contentList.OrderBy(c => c.Name).Last();
         var streamContent = await storage.OpenReadAsync(bobFile.FullPath);
         Assert.True(streamContent.CanRead && streamContent.Length > 0);
      }

      [Fact]
      public async void OpenWriteTest()
      {
         var storage = GetBlobStorage();
         var fileName = "test";
         var result = await storage.OpenWriteAsync(fileName);
         Assert.IsType<SftpFileStream>(result);
      }

      [Fact]
      public async void DeleteTest()
      {
         var storage = GetBlobStorage();
         var directoryName = "test";
         await storage.OpenWriteAsync(directoryName);
         try
         {
            await storage.DeleteAsync(directoryName);
            Assert.True(true);
         }
         catch(Exception e)
         {
            throw new ArgumentException($"'{directoryName}' folder or file can't be deleted. {e.Message}");
         }
      }

      [Fact]
      public async void ExistTest()
      {
         var storage = GetBlobStorage();
         var directoryName = "test";
         Assert.True(await storage.ExistsAsync(directoryName));
      }

      [Fact]
      public async void GetBlobTest()
      {
         var storage = GetBlobStorage();
         var directoryName = "in";
         Assert.IsType<Blob>(await storage.GetBlobAsync(directoryName));
      }

      [Fact]
      public async void MoveTest()
      {
         var storage = GetBlobStorage();

         var contentList = await storage.ListAsync(new ListOptions { FolderPath = "/in/" });
         var bobFile = contentList.OrderBy(c => c.Name).Last();
         var fromPath = StoragePath.Normalize(bobFile.FullPath);
         var toPath = $"archive/{StoragePath.Normalize(bobFile.Name)}";
         
         await storage.MoveBlobAsync(fromPath, toPath);
         Assert.True(await storage.ExistsAsync(toPath));
      }

      [Fact]
      public async void CreateContainerTest()
      {
         var storage = GetBlobStorage();
         await storage.SetBlobAsync(new Blob("new", BlobItemKind.Folder));
         StorageFactory.Blobs.FromConnectionString
      }
   }
}
