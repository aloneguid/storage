using System;
using Xunit;

namespace Storage.Net.Tests.Core
{
   public class StoragePathTest
   {
      [Theory]
      [InlineData("/dev/one", new[] { "dev", "one" })]
      [InlineData("/one/two/three", new[] { "one", "two", "three" })]
      [InlineData("/three", new[] { "one", "..", "three" })]
      public void Combine_theory(string expected, string[] parts)
      {
         Assert.Equal(expected, StoragePath.Combine(parts));
      }

      [Theory]
      [InlineData("dev/..", "/")]
      [InlineData("dev/../storage", "/storage")]
      [InlineData("/one", "/one")]
      [InlineData("/one/", "/one")]
      [InlineData("/one/../../../../", "/")]
      public void Normalize_theory(string path, string expected)
      {
         Assert.Equal(expected, StoragePath.Normalize(path));
      }

      [Theory]
      [InlineData("one/two/three", "/one/two")]
      [InlineData("one/two", "/one")]
      [InlineData("one/../two/three", "/two")]
      [InlineData("one/../two/three/four/..", "/two")]
      public void Get_parent_theory(string path, string expected)
      {
         Assert.Equal(expected, StoragePath.GetParent(path));
      }
   }
}
