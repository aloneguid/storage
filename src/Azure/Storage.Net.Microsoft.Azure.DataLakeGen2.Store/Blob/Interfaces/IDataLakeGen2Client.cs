using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Storage.Net.Microsoft.Azure.DataLakeGen2.Store.Blob.Models;

namespace Storage.Net.Microsoft.Azure.DataLakeGen2.Store.Blob.Interfaces
{
   public interface IDataLakeGen2Client
   {
      Task AppendFileAsync(string filesystem, string path, byte[] content, long position);
      Task CreateDirectoryAsync(string filesystem, string directory);
      Task CreateFileAsync(string filesystem, string path);
      Task CreateFilesystemAsync(string filesystem);
      Task DeleteDirectoryAsync(string filesystem, string path, bool isRecursive = true);
      Task DeleteFileAsync(string filesystem, string path);
      Task DeleteFilesystemAsync(string filesystem);
      Task FlushFileAsync(string filesystem, string path, long position);
      Task<AccessControl> GetAccessControlAsync(string filesystem, string path);
      Task<Properties> GetPropertiesAsync(string filesystem, string path);

      Task<DirectoryList> ListDirectoryAsync(string filesystem, string directory,
         bool isRecursive = false,
         int maxResults = 5000);

      Stream OpenRead(string filesystem, string path);
      Task<Stream> OpenWriteAsync(string filesystem, string path);
      Task<byte[]> ReadFileAsync(string filesystem, string path, long? start = null, long? end = null);
      Task SetAccessControlAsync(string filesystem, string path, List<AclItem> acl);
   }
}