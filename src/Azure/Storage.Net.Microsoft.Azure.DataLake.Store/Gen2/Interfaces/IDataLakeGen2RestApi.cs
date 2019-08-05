using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace Storage.Net.Microsoft.Azure.DataLakeGen2.Store.Gen2.Interfaces
{
   interface IDataLakeGen2RestApi
   {
      Task<HttpResponseMessage> AppendPathAsync(string filesystem, string path, byte[] content,
         long position, CancellationToken cancellationToken = default);

      Task<HttpResponseMessage> CreateFileAsync(string filesystem, string path,
         CancellationToken cancellationToken = default);

      Task<HttpResponseMessage> CreateDirectoryAsync(string filesystem, string path,
         CancellationToken cancellationToken = default);

      Task<HttpResponseMessage> CreateFilesystemAsync(string filesystem,
         CancellationToken cancellationToken = default);

      Task<HttpResponseMessage> DeleteFilesystemAsync(string filesystem,
         CancellationToken cancellationToken = default);

      Task<HttpResponseMessage> DeletePathAsync(string filesystem, string path, bool isRecursive,
         CancellationToken cancellationToken = default);

      Task<HttpResponseMessage> GetAccessControlAsync(string filesystem, string path,
         CancellationToken cancellationToken = default);

      Task<HttpResponseMessage> GetStatusAsync(string filesystem, string path,
         CancellationToken cancellationToken = default);

      Task<HttpResponseMessage> FlushPathAsync(string filesystem, string path, long position,
         CancellationToken cancellationToken = default);

      Task<HttpResponseMessage> ListFilesystemsAsync(int maxResults, CancellationToken cancellationToken = default);

      Task<HttpResponseMessage> ListPathAsync(string filesystem, string directory, bool isRecursive, int maxResults,
         CancellationToken cancellationToken = default);

      Task<HttpResponseMessage> ReadPathAsync(string filesystem, string path, long? start = null, long? end = null,
         CancellationToken cancellationToken = default);

      Task<HttpResponseMessage> SetAccessControlAsync(string filesystem, string path, string acl,
         CancellationToken cancellationToken = default);
   }
}