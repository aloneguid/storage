using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Storage.Net.Blobs;
using Xunit;

namespace Storage.Net.Tests.Blobs
{
   public class VirtualStorageTest
   {
      private readonly VirtualStorage _vs = new VirtualStorage();
      private readonly IBlobStorage _ms0 = StorageFactory.Blobs.InMemory();
      private readonly IBlobStorage _ms1 = StorageFactory.Blobs.InMemory();
      private readonly IBlobStorage _ms2 = StorageFactory.Blobs.InMemory();

      public VirtualStorageTest()
      {
         _vs.Mount("/", _ms0);
         _vs.Mount("/mnt/ms1", _ms1);
         _vs.Mount("/mnt/ms2", _ms2);
      }

      [Fact]
      public async Task Return_files_and_mounts_one_mount()
      {
         IReadOnlyCollection<Blob> all = await _vs.ListAsync();

         Assert.Equal(1, all.Count);   // "mnt" folder
         Assert.Equal(new Blob("/mnt", BlobItemKind.Folder), all.First());
      }

      [Fact]
      public async Task Return_files_and_mounts_one_mount_and_one_file()
      {
         await _ms0.WriteTextAsync("1.txt", "test");

         IReadOnlyCollection<Blob> all = await _vs.ListAsync();

         Assert.Equal(2, all.Count);   // "mnt" folder
         Assert.Equal(new Blob("/mnt", BlobItemKind.Folder), all.First());
         Assert.Equal(new Blob("1.txt"), all.Skip(1).First());
      }
   }
}
