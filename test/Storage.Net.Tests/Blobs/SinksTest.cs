using System.Threading.Tasks;
using Storage.Net.Blobs;
using Storage.Net.Blobs.Sinks.Impl;
using Xunit;

namespace Storage.Net.Tests.Blobs.Sink
{
   public class GzipSinkTest : SinksTest
   {
      public GzipSinkTest() : base(StorageFactory.Blobs.InMemory().WithGzipCompression())
      {

      }
   }

   public class SymmetricEncryptionTest : SinksTest
   {
      public SymmetricEncryptionTest() : base(
         StorageFactory.Blobs.InMemory().WithSymmetricEncryption("6qg/7EgPmrK9ZY70pnECtZ40g3dDe74czSvWJ+3dj0A="))
      {

      }
   }

   public class CompressedAndEncryptedTest : SinksTest
   {
      public CompressedAndEncryptedTest() : base(
         StorageFactory.Blobs
            .InMemory()
            .WithSinks(
               new GZipSink(),
               new SymmetricEncryptionSink("To6X5XVaNNMKFfxssJS6biREGpOVZjEIC6T7cc1rJF0=")))
      {

      }
   }

   public abstract class SinksTest
   {
      private readonly IBlobStorage storage;

      protected SinksTest(IBlobStorage storage)
      {
         this.storage = storage;
      }

      [Theory]
      [InlineData(null)]
      [InlineData("sample")]
      [InlineData("123")]
      [InlineData("123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123")]
      public async Task Roundtrip(string sample)
      {
         await storage.WriteTextAsync("target.txt", sample);

         Assert.Equal(sample, await storage.ReadTextAsync("target.txt"));
      }
   }
}
