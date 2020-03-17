using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Storage.Net.Blobs;
using Xunit;

namespace Storage.Net.Tests.Integration.Azure
{
   [Trait("Category", "Blobs")]
   public class LeakyDatabricksStorageTest
   {
      private readonly IBlobStorage _storage;

      public LeakyDatabricksStorageTest()
      {
         ITestSettings settings = Settings.Instance;
         _storage = StorageFactory.Blobs.Databricks(settings.DatabricksBaseUri, settings.DatabricksToken);
      }

      [Fact]
      public async Task List_root()
      {
         IReadOnlyCollection<Blob> roots = await _storage.ListAsync();

         Assert.Equal(2, roots.Count); // dbfs + notebooks
      }

      [Fact]
      public async Task List_dbfs_root()
      {
         IReadOnlyCollection<Blob> roots = await _storage.ListAsync("/dbfs");

         Assert.True(roots.Count > 0);
      }

      [Fact]
      public async Task List_notebooks_root()
      {
         IReadOnlyCollection<Blob> roots = await _storage.ListAsync("/notebooks");

         Assert.True(roots.Count > 0);
      }
   }
}
