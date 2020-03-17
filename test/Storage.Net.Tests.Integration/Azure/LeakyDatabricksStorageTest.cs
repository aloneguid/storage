using System;
using System.Collections.Generic;
using System.Text;
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



   }
}
