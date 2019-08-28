using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Storage.Net.Microsoft.Azure.DataLake.Store;
using Xunit;

namespace Storage.Net.Tests.Integration.Azure
{
   public class GraphClientTest
   {
      [Fact]
      public async Task Smoke()
      {
         var s = new GraphService(Settings.Instance.AzureDataLakeGen2TenantId, Settings.Instance.AzureDataLakeGen2PrincipalId, Settings.Instance.AzureDataLakeGen2PrincipalSecret);

         await s.DoAsync();
      }
   }
}
