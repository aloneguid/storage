using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Storage.Net.Microsoft.Azure.DataLakeGen2.Store.Blob.Models;

namespace Storage.Net.Microsoft.Azure.DataLakeGen2.Store.Blob.Interfaces
{
   public interface IDataLakeGen2Client
   {
      Task AppendFileAsync(string filesystem, string path, byte[] content, long position,
         CancellationToken cancellationToken = default);

      Task CreateDirectoryAsync(string filesystem, string directory, CancellationToken cancellationToken = default);
      Task CreateFileAsync(string filesystem, string path, CancellationToken cancellationToken = default);
      Task CreateFilesystemAsync(string filesystem, CancellationToken cancellationToken = default);

      Task DeleteDirectoryAsync(string filesystem, string path, bool isRecursive = true,
         CancellationToken cancellationToken = default);

      Task DeleteFileAsync(string filesystem, string path, CancellationToken cancellationToken = default);
      Task DeleteFilesystemAsync(string filesystem, CancellationToken cancellationToken = default);
      Task FlushFileAsync(string filesystem, string path, long position, CancellationToken cancellationToken = default);

      Task<AccessControl> GetAccessControlAsync(string filesystem, string path,
         CancellationToken cancellationToken = default);

      Task<Properties> GetPropertiesAsync(string filesystem, string path,
         CancellationToken cancellationToken = default);

      Task<DirectoryList> ListDirectoryAsync(string filesystem, string directory,
         bool isRecursive = false, int maxResults = 5000, CancellationToken cancellationToken = default);

      Stream OpenRead(string filesystem, string path);
      Task<Stream> OpenWriteAsync(string filesystem, string path, CancellationToken cancellationToken = default);

      Task<byte[]> ReadFileAsync(string filesystem, string path, long? start = null, long? end = null,
         CancellationToken cancellationToken = default);

      Task SetAccessControlAsync(string filesystem, string path, List<AclItem> acl,
         CancellationToken cancellationToken = default);
   }
}