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

      [Fact]
      public void Construct_with_prefix()
      {
         var cs = new StorageConnectionString("disk");

         Assert.Equal("disk", cs.Prefix);
         Assert.Empty(cs.Parameters);
      }

      [Fact]
      public void Build_with_parameter_map()
      {
         var cs = new StorageConnectionString("aws.s3");
         cs.Parameters["key1"] = "value1";
         cs.Parameters["key2"] = "value2";

         Assert.Equal("aws.s3://key1=value1;key2=value2", cs.ToString());
      }

      [Fact]
      public void Parameter_with_no_value()
      {
         var cs = new StorageConnectionString("local://account=my;msi");

         Assert.True(cs.Parameters.ContainsKey("msi"));
      }
   }
}