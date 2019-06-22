using System.Net.Http;
using System.Threading.Tasks;

namespace Storage.Net.Microsoft.Azure.DataLakeGen2.Store.Blob.Interfaces
{
   public interface IDataLakeGen2RestApi
   {
      Task<HttpResponseMessage> AppendPathAsync(string filesystem, string path, byte[] content,
         long position);

      Task<HttpResponseMessage> CreateFileAsync(string filesystem, string path);
      Task<HttpResponseMessage> CreateDirectoryAsync(string filesystem, string path);
      Task<HttpResponseMessage> CreateFilesystemAsync(string filesystem);
      Task<HttpResponseMessage> DeleteFilesystemAsync(string filesystem);
      Task<HttpResponseMessage> DeletePathAsync(string filesystem, string path, bool isRecursive);
      Task<HttpResponseMessage> GetAccessControlAsync(string filesystem, string path);
      Task<HttpResponseMessage> GetStatusAsync(string filesystem, string path);
      Task<HttpResponseMessage> FlushPathAsync(string filesystem, string path, long position);
      Task<HttpResponseMessage> ListPathAsync(string filesystem, string directory, bool isRecursive, int maxResults);
      Task<HttpResponseMessage> ReadPathAsync(string filesystem, string path, long? start = null, long? end = null);
      Task<HttpResponseMessage> SetAccessControlAsync(string filesystem, string path, string acl);
   }
}