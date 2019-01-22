using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Storage.Net.ConnectionString;
using Xunit;

namespace Storage.Net.Tests
{
   public class StorageConnectionStringTest
   {
      [Fact]
      public void Ideal_connection_string_parsed()
      {
         string cs = "azure.blob://account=accname;key=keywithequals==;container=me";

         var scs = new StorageConnectionString(cs);

         Assert.Equal(cs, scs.ConnectionString);

         scs.GetRequired("account", false, out string account);
         scs.GetRequired("key", false, out string key);
         scs.GetRequired("container", false, out string container);

         Assert.Equal("accname", account);
         Assert.Equal("keywithequals==", key);
         Assert.Equal("me", container);
      }
   }
}