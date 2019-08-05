using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Storage.Net.Microsoft.Azure.DataLakeGen2.Store.Gen2.Interfaces;
using Storage.Net.Microsoft.Azure.DataLake.Store.Gen2.Models;

namespace Storage.Net.Microsoft.Azure.DataLakeGen2.Store.Gen2.BLL
{
   class DataLakeGen2Client
   {
      private readonly IDataLakeGen2RestApi _restApi;

      public DataLakeGen2Client(HttpClient httpClient, IAuthorisation authorisation, string storageAccountName) :
         this(new DataLakeGen2RestApi(httpClient, authorisation, storageAccountName))
      {
      }

      public DataLakeGen2Client(IDataLakeGen2RestApi restApi)
      {
         _restApi = restApi;
      }

      public async Task AppendFileAsync(string filesystem, string path, byte[] content, long position,
         CancellationToken cancellationToken = default)
      {
         await _restApi.AppendPathAsync(filesystem, path, content, position, cancellationToken);
      }

      public Task CreateDirectoryAsync(string filesystem, string directory,
         CancellationToken cancellationToken = default)
      {
         return _restApi.CreateDirectoryAsync(filesystem, directory, cancellationToken);
      }

      public Task CreateFileAsync(string filesystem, string path, CancellationToken cancellationToken = default)
      {
         return _restApi.CreateFileAsync(filesystem, path, cancellationToken);
      }

      public Task CreateFilesystemAsync(string filesystem, CancellationToken cancellationToken = default)
      {
         return _restApi.CreateFilesystemAsync(filesystem, cancellationToken);
      }

      public Task DeleteDirectoryAsync(string filesystem, string path, bool isRecursive = true,
         CancellationToken cancellationToken = default)
      {
         return _restApi.DeletePathAsync(filesystem, path, isRecursive, cancellationToken);
      }

      public Task DeleteFileAsync(string filesystem, string path, CancellationToken cancellationToken = default)
      {
         return _restApi.DeletePathAsync(filesystem, path, false, cancellationToken);
      }

      public Task DeleteFilesystemAsync(string filesystem, CancellationToken cancellationToken = default)
      {
         return _restApi.DeleteFilesystemAsync(filesystem, cancellationToken);
      }

      public Task FlushFileAsync(string filesystem, string path, long position,
         CancellationToken cancellationToken = default)
      {
         return _restApi.FlushPathAsync(filesystem, path, position, cancellationToken);
      }

      public async Task<AccessControl> GetAccessControlAsync(string filesystem, string path,
         CancellationToken cancellationToken = default)
      {
         HttpResponseMessage result = await _restApi.GetAccessControlAsync(filesystem, path, cancellationToken);

         return new AccessControl
         {
            Acl = DeserialiseAcl(result.Headers.GetValues("x-ms-acl").First()).ToList(),
            Group = result.Headers.GetValues("x-ms-group").First(),
            Owner = result.Headers.GetValues("x-ms-owner").First(),
            Permissions = result.Headers.GetValues("x-ms-permissions").First()
         };
      }

      public async Task<Properties> GetPropertiesAsync(string filesystem, string path,
         CancellationToken cancellationToken = default)
      {
         HttpResponseMessage result = await _restApi.GetStatusAsync(filesystem, path, cancellationToken);

         return new Properties
         {
            ContentType = result.Content.Headers.ContentType,
            LastModified = result.Content.Headers.LastModified.GetValueOrDefault(),
            Length = result.Content.Headers.ContentLength.GetValueOrDefault(),
            IsDirectory = result.Headers.GetValues("x-ms-resource-type").First() == "directory"
         };
      }

      public async Task<DirectoryList> ListDirectoryAsync(string filesystem, string directory,
         bool isRecursive = false, int maxResults = 5000, CancellationToken cancellationToken = default)
      {
         HttpResponseMessage result =
            await _restApi.ListPathAsync(filesystem, directory, isRecursive, maxResults, cancellationToken);
         string content = await result.Content.ReadAsStringAsync();

         return JsonConvert.DeserializeObject<DirectoryList>(content);
      }

      public async Task<FilesystemList> ListFilesystemsAsync(int maxResults = 5000, CancellationToken cancellationToken = default)
      {
         HttpResponseMessage result =
            await _restApi.ListFilesystemsAsync(maxResults, cancellationToken);
         string content = await result.Content.ReadAsStringAsync();

         return JsonConvert.DeserializeObject<FilesystemList>(content);
      }

      public async Task<byte[]> ReadFileAsync(string filesystem, string path, long? start = null, long? end = null,
         CancellationToken cancellationToken = default)
      {
         HttpResponseMessage result = await _restApi.ReadPathAsync(filesystem, path, start, end, cancellationToken);
         return await result.Content.ReadAsByteArrayAsync();
      }

      public Task SetAccessControlAsync(string filesystem, string path, List<AclItem> acl,
         CancellationToken cancellationToken = default)
      {
         return _restApi.SetAccessControlAsync(filesystem, path, SerialiseAcl(acl), cancellationToken);
      }

      public static DataLakeGen2Client Create(string storageAccount, string sharedAccessKey)
      {
         return new DataLakeGen2Client(new HttpClient(),
            new SharedAccessKeyAuthorisation(sharedAccessKey),
            storageAccount
         );
      }

      public static DataLakeGen2Client Create(string storageAccount, string tenantId, string clientId,
         string clientSecret)
      {
         return new DataLakeGen2Client(new HttpClient(),
            new ActiveDirectoryAuthorisation(tenantId, clientId, clientSecret),
            storageAccount
         );
      }

      public Stream OpenRead(string filesystem, string path)
      {
         return new DataLakeGen2Stream(this, filesystem, path);
      }

      public async Task<Stream> OpenWriteAsync(string filesystem, string path,
         CancellationToken cancellationToken = default)
      {
         try
         {
            Properties properties = await GetPropertiesAsync(filesystem, path, cancellationToken);
         }
         catch(DataLakeGen2Exception e)
         {
            if(e.StatusCode == HttpStatusCode.NotFound)
            {
               await CreateFileAsync(filesystem, path, cancellationToken);
            }
         }

         return new DataLakeGen2Stream(this, filesystem, path);
      }

      private static string SerialiseAcl(IEnumerable<AclItem> acl)
      {
         return string.Join(",", acl
            .Select(x => new[]
            {
               new
               {
                  IsDefault = true,
                  AccessControl = x.Default,
                  x.User
               },
               new
               {
                  IsDefault = false,
                  AccessControl = x.Access,
                  x.User
               }
            })
            .SelectMany(x => x)
            .Where(x => x.AccessControl != null)
            .OrderBy(x => x.User)
            .ThenBy(x => x.IsDefault)
            .Select(x => new
            {
               Read = x.AccessControl.Read ? "r" : "-",
               Write = x.AccessControl.Write ? "w" : "-",
               Execute = x.AccessControl.Execute ? "x" : "-",
               x.IsDefault,
               x.User
            })
            .Select(x => $"{(x.IsDefault ? "default:" : "")}{x.User}:{x.Read}{x.Write}{x.Execute}"));
      }

      private static IEnumerable<AclItem> DeserialiseAcl(string acl)
      {
         const string defaultTag = "default:";
         return acl.Split(',')
            .Select(x => new
            {
               IsDefault = x.StartsWith(defaultTag),
               Value = x
            })
            .Select(x => new
            {
               x.IsDefault,
               Value = x.IsDefault ? x.Value.Substring(defaultTag.Length) : x.Value
            })
            .Select(x => new
            {
               x.IsDefault,
               x.Value,
               SplitIndex = x.Value.LastIndexOf(':')
            })
            .Select(x => new
            {
               x.IsDefault,
               User = x.Value.Substring(0, x.SplitIndex),
               AccessControl = x.Value.Substring(x.SplitIndex + 1, x.Value.Length - x.SplitIndex - 1)
            })
            .GroupBy(x => x.User)
            .Select(x => new
            {
               User = x.Key,
               Access = x.FirstOrDefault(y => !y.IsDefault)?.AccessControl,
               Default = x.FirstOrDefault(y => y.IsDefault)?.AccessControl
            })
            .Select(x => new AclItem
            {
               Access = x.Access == null
                  ? null
                  : new AclPermission
                  {
                     Read = x.Access[0] == 'r',
                     Write = x.Access[1] == 'w',
                     Execute = x.Access[2] == 'x'
                  },
               Default = x.Default == null
                  ? null
                  : new AclPermission
                  {
                     Read = x.Default[0] == 'r',
                     Write = x.Default[1] == 'w',
                     Execute = x.Default[2] == 'x'
                  },
               User = x.User
            })
            .OrderBy(x => x.User)
            .ThenBy(x => x.Default == null);
      }
   }
}