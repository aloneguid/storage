using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Storage.Net.Blob;
using Storage.Net.Blob.Files;
using Storage.Net.Microsoft.Azure.Storage.Blob;
using Storage.Net.Tests.Integration;
using Xunit;

namespace Storage.Net.Tests
{
   public class StorageInstantiationTest : AbstractTestFixture
   {
      public StorageInstantiationTest()
      {
         StorageFactory.Modules.UseAzureStorage();
      }

      [Fact]
      public void Disk_storage_creates_from_connection_string()
      {
         IBlobStorage disk = StorageFactory.Blobs.FromConnectionString("disk://path=" + TestDir.FullName);

         Assert.IsType<DiskDirectoryBlobStorage>(disk);

         DiskDirectoryBlobStorage ddbs = (DiskDirectoryBlobStorage)disk;

         Assert.Equal(TestDir.FullName, ddbs.RootDirectory.FullName);
      }
   }
}
