using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Polly;
using Renci.SshNet;
using Renci.SshNet.Sftp;
using Storage.Net.Blobs;
using Storage.Net.SFtp.Extensions;

namespace Storage.Net.SFtp
{
   public class SshNetSFtpBlobStorage : IBlobStorage
   {
      private readonly ConnectionInfo _connectionInfo;
      private readonly ILogger<SshNetSFtpBlobStorage> _logger;

      public int MaxRetryCount { get; set; } = 3;

      public SshNetSFtpBlobStorage(string host, ushort port, string user, string password, ILogger<SshNetSFtpBlobStorage> logger = null)
      {
         _connectionInfo = new PasswordConnectionInfo(host, port, user, password);
         _logger = logger ?? NullLogger<SshNetSFtpBlobStorage>.Instance;
      }

      public void Dispose() { }

      protected virtual SftpClient CreateClient()
      {
         var client = new SftpClient(_connectionInfo);
         client.HostKeyReceived += (sender, args) => { };
         try
         {
            client.Connect();
         }
         catch (Exception e)
         {
            _logger.LogError("Failure connecting to host: {0}:{1}. {2}", _connectionInfo.Host, _connectionInfo.Port, e.Message);
            throw;
         }
         _logger.LogInformation("Connected to host: {0}:{1}", _connectionInfo.Host, _connectionInfo.Port);
         return client;
      }

      public Task DeleteAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default(CancellationToken))
      {
         using (var sftpClient = CreateClient())
         {
            foreach (string path in fullPaths)
            {
               sftpClient.Delete(path);
               _logger.LogInformation("{0} deleted", path);
            }
         }
         _logger.LogTrace("Deleted a total of {0} remote resources", fullPaths.Count());
         return Task.CompletedTask;
      }

      public Task<IReadOnlyCollection<bool>> ExistsAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default(CancellationToken))
      {
         _logger.LogTrace("Checking if paths exist");
         using(var sftpClient = CreateClient())
         {
            return Task.FromResult<IReadOnlyCollection<bool>>(fullPaths.Select(path =>
            {
               var pathExists = sftpClient.Exists(path);
               _logger.LogTrace("{0} {1}", path, (pathExists ? "exists" : "not found"));
               return pathExists;
            }).ToList());
         }
      }

      public async Task<IReadOnlyCollection<Blob>> GetBlobsAsync(IEnumerable<string> fullPaths, CancellationToken cancellationToken = default(CancellationToken))
      {
         using (var sftpClient = CreateClient())
         {
            var files = new List<Blob>();
            foreach (var fullPath in fullPaths.GroupBy(g => StoragePath.GetParent(g)))
            {
               var fullPathNormalized = StoragePath.Normalize(fullPath.SingleOrDefault(), true);

               if(cancellationToken.IsCancellationRequested) break;
               var listing = await sftpClient.ListDirectoryAsync(fullPath.Key);
               files.AddRange(listing.Where(l => (l.IsDirectory || l.IsRegularFile) && fullPathNormalized == l.FullName).Select(SftpExtensions.ToBlobId));
            }
            return files;
         }
      }

      public async Task<IReadOnlyCollection<Blob>> ListAsync(ListOptions options = null, CancellationToken cancellationToken = default(CancellationToken))
      {
         options = options ?? new ListOptions();
         _logger.LogTrace("Listing folder contents: {0}", options.FolderPath);
         var directoryContents = Enumerable.Empty<SftpFile>();
         using(var sftpClient = CreateClient())
         {
            directoryContents = await sftpClient.ListDirectoryAsync(options.FolderPath);
         }
         var listResults = directoryContents
            .Where(dc => (options.FilePrefix == null || dc.FullName.StartsWith(options.FilePrefix)) && 
            (dc.IsDirectory || dc.IsRegularFile || dc.OwnerCanRead) && 
            !cancellationToken.IsCancellationRequested && 
            (dc.Name != "." && dc.Name != ".."))
            .Take(options.MaxResults ?? int.MaxValue)
            .Select(SftpExtensions.ToBlobId)
            .Where(options.BrowseFilter != null ? options.BrowseFilter : (filter) => true)
            .Select(blob =>
            {
               _logger.LogInformation("{0}\t{#,###:1}", blob.FullPath, blob.Size);
               return blob;
            })
            .ToList();
         _logger.LogInformation("{0} Total Resources", listResults.Count());         
         return listResults;
      }

      public async Task<Stream> OpenReadAsync(string fullPath, CancellationToken cancellationToken = default(CancellationToken))
      {
         byte[] fileBytes;
         using(var sftpClient = CreateClient())
         {
            fileBytes = await Task.FromResult<byte[]>(Policy.Handle<Exception>().Retry(MaxRetryCount, (e, t) =>
            {
               _logger.LogError(e, "Try: {0} - Failed opening resource {1} for reading", t, fullPath);
            }).Execute(() => sftpClient.ReadAllBytes(fullPath)));
         }
         return new MemoryStream(fileBytes);
      }

      public Task<ITransaction> OpenTransactionAsync() => Task.FromResult(EmptyTransaction.Instance);

      public Task<Stream> OpenWriteAsync(string fullPath, bool append = false, CancellationToken cancellationToken = default(CancellationToken))
      {
         using(var sftpClient = CreateClient())
         {
            return Task.FromResult<Stream>(Policy.Handle<Exception>().Retry(MaxRetryCount, (e, t) =>
            {
               _logger.LogError(e, "Try: {0} - Failed opening resource {1} for writing", t, fullPath);
            }).Execute(() => sftpClient.OpenWrite(fullPath)));
         }
      }

      public Task SetBlobsAsync(IEnumerable<Blob> blobs, CancellationToken cancellationToken = default(CancellationToken))
      {
         foreach(Blob blob in blobs)
         {
            if(blob.IsFolder)
            {
               using(var sftpClient = CreateClient())
               {
                  sftpClient.CreateDirectory(blob.FullPath);
               }
            }
            else if(blob.IsFile)
            {
               using(var sftpClient = CreateClient())
               {
                  var file = sftpClient.Create(blob.FullPath);
                  file.Close();
               }
            }
            else
            {
               continue;
            }
          
         }
         return Task.CompletedTask;
      }

      public Task MoveBlobAsync(string fromPath, string toPath, CancellationToken cancellationToken = default(CancellationToken))
      {
         _logger.LogInformation("Moving blob from {0} to {1}", fromPath, toPath);
         using(var sftpClient = CreateClient())
         {
            sftpClient.RenameFile(StoragePath.Normalize(fromPath), StoragePath.Normalize(toPath));
         }
         return Task.CompletedTask;
      }
   }
}
