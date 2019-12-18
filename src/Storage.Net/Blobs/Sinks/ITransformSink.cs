using System.IO;

namespace Storage.Net.Blobs.Sinks
{
   /// <summary>
   /// Data transformation sink that can transform both read and write streams on <see cref="IBlobStorage"/>
   /// </summary>
   public interface ITransformSink
   {
      /// <summary>
      /// Opens a stream for reading based on opened original stream
      /// </summary>
      /// <param name="fullPath">Full path to file</param>
      /// <param name="parentStream">Parent stream that is already open</param>
      /// <returns></returns>
      Stream OpenReadStream(string fullPath, Stream parentStream);

      /// <summary>
      /// Opens a stream for writing based on opened original stream
      /// </summary>
      /// <param name="fullPath">Full path to file</param>
      /// <param name="parentStream">Source stream to write</param>
      /// <returns></returns>
      Stream OpenWriteStream(string fullPath, Stream parentStream);
   }
}
