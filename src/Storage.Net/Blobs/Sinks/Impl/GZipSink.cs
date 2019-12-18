using System.IO;
using System.IO.Compression;

namespace Storage.Net.Blobs.Sinks.Impl
{
   /// <summary>
   /// GZip transformation sink
   /// </summary>
   public class GZipSink : ITransformSink
   {
      private readonly CompressionLevel _compressionLevel;

      /// <summary>
      /// 
      /// </summary>
      /// <param name="compressionLevel"></param>
      public GZipSink(CompressionLevel compressionLevel = CompressionLevel.Optimal)
      {
         _compressionLevel = compressionLevel;
      }

      /// <summary>
      /// 
      /// </summary>
      public Stream OpenReadStream(string fullPath, Stream parentStream)
      {
         if(parentStream == null)
            return null;

         return new GZipStream(parentStream, CompressionMode.Decompress, false);
      }

      /// <summary>
      /// 
      /// </summary>
      public Stream OpenWriteStream(string fullPath, Stream parentStream)
      {
         return new GZipStream(parentStream, _compressionLevel, false);
      }
   }
}
