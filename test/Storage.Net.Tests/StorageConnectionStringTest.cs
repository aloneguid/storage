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
         Assert.False(scs.IsNative);
         Assert.Null(scs.Native);
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

      [Fact]
      public void Native_Parsed()
      {
         const string native = "t=6;iiiifldjfljd fla dfj;;df";

         var cs = new StorageConnectionString("local://native=" + native);

         Assert.Equal("local", cs.Prefix);
         Assert.Single(cs.Parameters);
         Assert.True(cs.IsNative);
         Assert.Equal(native, cs.Native);
         Assert.Equal(native, cs.Parameters["native"]);

         //convert back to string
         string css = cs.ToString();
         Assert.Equal("local://native=" + native, css);

      }

      [Theory]
      [InlineData("va=lue")]
      [InlineData("va;lue")]
      public void Handles_special_characters(string valueToSave)
      {
         var cs = new StorageConnectionString("local://");
         cs["key"] = valueToSave;

         string css = cs.ToString();

         cs = new StorageConnectionString(css);
         Assert.Equal(valueToSave, cs["key"]);
      }
   }
}