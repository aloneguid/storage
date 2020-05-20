using System;
using System.Collections.Generic;
using System.Text;
using Storage.Net.Blobs;
using Xunit;

namespace Storage.Net.Tests.Core
{
   public class BlobTest
   {
      [Fact]
      public void Is_root_folder_for_root_folder()
      {
         Assert.True(new Blob("/", BlobItemKind.Folder).IsRootFolder);
      }

      [Fact]
      public void Is_root_folder_for_non_root_folder()
      {
         Assert.False(new Blob("/awesome", BlobItemKind.Folder).IsRootFolder);
      }

      [Theory]
      [InlineData("/", null, "/")]
      [InlineData("/f0/f1", "f2", "f2/f0")]
      public void Prepend_path(string path, string prefix, string expected)
      {
         var blob = new Blob(path, BlobItemKind.Folder);
         blob.PrependPath(prefix);
         Assert.Equal(expected, blob.FolderPath);
      }
   }
}
