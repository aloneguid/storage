using System.Collections.Generic;
using System.Threading.Tasks;
using Renci.SshNet;
using Renci.SshNet.Sftp;
using Storage.Net.Blobs;

namespace Storage.Net.SFtp
{
   public static class SFtpClientExtensions
    {
      public static async Task<IEnumerable<SftpFile>> ListDirectoryAsync(this SftpClient sftpClient, string folderPath = ".")
      {
         var result = sftpClient.BeginListDirectory(folderPath, null, null);
         return await Task.Factory.FromAsync(result, sftpClient.EndListDirectory);
      }

      public static Blob ToBlobId(this SftpFile file) => (file.IsDirectory || file.IsRegularFile || file.OwnerCanRead)
         ? new Blob(file.FullName, file.IsDirectory ? BlobItemKind.Folder : BlobItemKind.File) { Size = file.Length, LastModificationTime = file.LastWriteTime }
         : null;
   }
}
