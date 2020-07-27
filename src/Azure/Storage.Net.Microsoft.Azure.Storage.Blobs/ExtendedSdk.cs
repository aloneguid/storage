using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Core;
using Azure.Core.Pipeline;
using Azure.Storage.Blobs;
using Blobs.Gen2;
using NetBox.Extensions;
using Storage.Net;
using Storage.Net.Blobs;
using Storage.Net.Microsoft.Azure.Storage.Blobs;
using Storage.Net.Microsoft.Azure.Storage.Blobs.Gen2.Model;

namespace Blobs
{
   class ExtendedSdk
   {
      private readonly BlobServiceClient _sdkClient;
      private readonly HttpPipeline _httpPipeline;
      private readonly string _dfsBaseAddress;
      private static readonly JsonSerializerOptions _jo = new JsonSerializerOptions
      {
         PropertyNameCaseInsensitive = true
      };

      static ExtendedSdk()
      {
         _jo.Converters.Add(new DateTimeConverterUsingDateTimeParse());
      }

      public ExtendedSdk(BlobServiceClient sdkClient, string accountName)
      {
         _sdkClient = sdkClient ?? throw new ArgumentNullException(nameof(sdkClient));

         _httpPipeline = GetHttpPipeline(sdkClient);
         _dfsBaseAddress = GetDfsBaseAddress(accountName);
      }

      private static string GetDfsBaseAddress(string accountName) => 
         $"https://{accountName}.dfs.core.windows.net/";

      private static HttpPipeline GetHttpPipeline(BlobServiceClient sdkClient)
      {
         PropertyInfo httpPipelineProperty = 
            typeof(BlobServiceClient).GetProperty("Pipeline", BindingFlags.NonPublic | BindingFlags.Instance);

         HttpPipeline httpPipeline = httpPipelineProperty.GetValue(sdkClient) as HttpPipeline;

         if(httpPipeline == null)
            throw new ArgumentException($"cannot find {typeof(HttpPipeline)} via reflection");

         return httpPipeline;
      }

      public async Task<IReadOnlyCollection<Filesystem>> ListFilesystemsAsync(CancellationToken cancellationToken)
      {
         FilesystemList response = await InvokeAsync<FilesystemList>(
            "?resource=account",
            RequestMethod.Get,
            cancellationToken).ConfigureAwait(false);

         return response.Filesystems;
      }

      public async Task<IReadOnlyCollection<Blob>> ListFilesystemsAsBlobsAsync(CancellationToken cancellationToken)
      {
         IReadOnlyCollection<Filesystem> fss = await ListFilesystemsAsync(cancellationToken).ConfigureAwait(false);

         return fss.Select(AzConvert.ToBlob).ToList();
      }

      public async Task CreateFilesystemAsync(string name, CancellationToken cancellationToken)
      {
         try
         {
            await InvokeAsync<Void>($"{name}?resource=filesystem", RequestMethod.Put, cancellationToken)
               .ConfigureAwait(false);
         }
         catch(RequestFailedException ex) when (ex.ErrorCode == "FilesystemAlreadyExists")
         {

         }
      }

      public async Task DeleteFilesystemAsync(string name, CancellationToken cancellationToken)
      {
         try
         {
            await InvokeAsync<Void>($"{name}?resource=filesystem", RequestMethod.Delete, cancellationToken)
               .ConfigureAwait(false);
         }
         catch(RequestFailedException ex) when (ex.ErrorCode == "FilesystemNotFound")
         {

         }
      }

      public async Task SetAccessControlAsync(
         string fullPath,
         AccessControl accessControl,
         CancellationToken cancellationToken)
      {
         DecomposePath(fullPath, out string filesystemName, out string relativePath, false);

         await InvokeAsync<Void>(
            $"{filesystemName}/{relativePath}?action=setAccessControl",
            RequestMethod.Patch,
            cancellationToken,
            new Dictionary<string, string>
            {
               ["x-ms-acl"] = accessControl.ToString()
            }).ConfigureAwait(false);
      }

      public async Task<AccessControl> GetAccessControlAsync(
         string fullPath,
         bool getUpn,
         CancellationToken cancellationToken)
      {
         DecomposePath(fullPath, out string filesystemName, out string relativePath, false);

         (Void _, IDictionary<string, string> headers) = await InvokeExtraAsync<Void>(
            $"{filesystemName}/{relativePath}?action=getAccessControl&upn={getUpn}",
            RequestMethod.Head,
            cancellationToken).ConfigureAwait(false);

         headers.TryGetValue("x-ms-owner", out string owner);
         headers.TryGetValue("x-ms-group", out string group);
         headers.TryGetValue("x-ms-permissions", out string permissions);
         headers.TryGetValue("x-ms-acl", out string acl);

         return new AccessControl(owner, group, permissions, acl);
      }

      public async Task DeleteAsync(string fullPath, CancellationToken cancellationToken)
      {
         DecomposePath(fullPath, out string fs, out string rp, false);

         if(StoragePath.IsRootPath(rp))
         {
            await DeleteFilesystemAsync(fs, cancellationToken).ConfigureAwait(false);
         }
         else
         {
            try
            {
               await InvokeAsync<Void>(
                  $"{fs}/{rp}?recursive=true",
                  RequestMethod.Delete,
                  cancellationToken).ConfigureAwait(false);
            }
            catch(RequestFailedException ex) when (ex.ErrorCode == "PathNotFound")
            {
               // file not found, ignore
            }
         }

      }

      public async Task<Blob> GetBlobAsync(string fullPath, CancellationToken cancellationToken)
      {
         DecomposePath(fullPath, out string fs, out string rp, false);

         if(StoragePath.IsRootPath(rp))
         {
            try
            {
               (Void _, IDictionary<string, string> headers) = await InvokeExtraAsync<Void>(
                  $"{fs}?resource=filesystem",
                  RequestMethod.Head,
                  cancellationToken).ConfigureAwait(false);

               return AzConvert.ToBlob(fullPath, headers, true);
            }
            catch(RequestFailedException ex) when (ex.ErrorCode == "FilesystemNotFound")
            {
               //filesystem doesn't exist
               return null;
            }
         }

         try
         {
            (Void _, IDictionary<string, string> fheaders) = await InvokeExtraAsync<Void>(
               $"{fs}/{rp.UrlEncode()}?action=getProperties",
               RequestMethod.Head,
               cancellationToken).ConfigureAwait(false);

            return AzConvert.ToBlob(fullPath, fheaders, false);
         }
         catch(RequestFailedException ex) when(ex.ErrorCode == "PathNotFound")
         {
            return null;
         }
      }

      #region [ Native Browsing ]

      public async Task<IReadOnlyCollection<Blob>> ListAsync(
         ListOptions options, CancellationToken cancellationToken)
      {
         if(options == null)
            options = new ListOptions();

         IReadOnlyCollection<Blob> result = await InternalListAsync(options, cancellationToken).ConfigureAwait(false);

         if(options.IncludeAttributes)
         {
            result = await Task.WhenAll(result.Select(b => GetWithMetadata(b, cancellationToken))).ConfigureAwait(false);
         }

         return result;
      }

      private Task<Blob> GetWithMetadata(Blob b, CancellationToken cancellationToken)
      {
         if(b.IsFile)
         {
            return GetBlobAsync(b, cancellationToken);
         }

         return Task.FromResult(b);
      }

      public async Task<IReadOnlyCollection<Blob>> InternalListAsync(ListOptions options, CancellationToken cancellationToken)
      {
         if(StoragePath.IsRootPath(options.FolderPath))
         {
            //only filesystems are in the root path
            var result = new List<Blob>(await ListFilesystemsAsBlobsAsync(cancellationToken).ConfigureAwait(false));

            if(options.Recurse)
            {
               foreach(Blob folder in result.Where(b => b.IsFolder).ToList())
               {
                  int? maxResults = options.MaxResults == null
                     ? null
                     : (int?)(options.MaxResults.Value - result.Count);

                  result.AddRange(await ListPathAsync(folder, maxResults, options, cancellationToken).ConfigureAwait(false));
               }
            }

            return result;
         }
         else
         {
            return await ListPathAsync(options.FolderPath, options.MaxResults, options, cancellationToken).ConfigureAwait(false);
         }
      }

      private async Task<IReadOnlyCollection<Blob>> ListPathAsync(string path, int? maxResults, ListOptions options, CancellationToken cancellationToken)
      {
         //get filesystem name and folder path
         string[] parts = StoragePath.Split(path);

         string fs = parts[0];
         string relativePath = StoragePath.Normalize(StoragePath.Combine(parts.Skip(1)), true);

         var list = new List<Gen2Path>();

         try
         {
            string continuation = null;
            do
            {
               string continuationParam = continuation == null ? null : $"&continuation={continuation.UrlEncode()}";

               (PathList pl, IDictionary<string, string> responseHeaders) = await InvokeExtraAsync<PathList>(
                  $"{fs}?resource=filesystem&directory={relativePath.UrlEncode()}&recursive={options.Recurse}{continuationParam}",
                  RequestMethod.Get,
                  cancellationToken).ConfigureAwait(false);

               list.AddRange(pl.Paths);

               responseHeaders.TryGetValue("x-ms-continuation", out continuation);
            } while(continuation != null);
         }
         catch(RequestFailedException ex) when (ex.ErrorCode == "PathNotFound" || ex.ErrorCode == "FilesystemNotFound")
         {
            // trying to list a path which doesn't exist, just return an empty result
            return new List<Blob>();
         }

         IEnumerable<Blob> result = list.Select(p => AzConvert.ToBlob(fs, p));

         if(options.FilePrefix != null)
            result = result.Where(b => b.IsFolder || b.Name.StartsWith(options.FilePrefix));

         if(options.BrowseFilter != null)
            result = result.Where(b => options.BrowseFilter(b));

         if(maxResults != null)
            result = result.Take(maxResults.Value);

         return result.ToList();
      }

      #endregion

      private HttpMessage CreateHttpMessage(
         string relativeUrl,
         RequestMethod method,
         IDictionary<string, string> headers = null)
      {
         HttpMessage message = _httpPipeline.CreateMessage();
         Request request = message.Request;

         var url = new Uri($"{_dfsBaseAddress}{relativeUrl}");

         request.Uri.Reset(url);
         request.Method = method;
         request.Headers.SetValue("x-ms-version", "2019-02-02");

         if(headers != null)
         {
            foreach(KeyValuePair<string, string> header in headers)
            {
               request.Headers.SetValue(header.Key, header.Value);
            }
         }

         return message;
      }

      private async Task<TResult> DeserialiseAsync<TResult>(HttpMessage message)
      {
         if(typeof(Void) == typeof(TResult))
            return default;

         string json;
         using(var src = new StreamReader(message.Response.ContentStream))
         {
            json = await src.ReadToEndAsync().ConfigureAwait(false);
         }

         return JsonSerializer.Deserialize<TResult>(json, _jo);
      }

      private async Task<TResult> InvokeAsync<TResult>(
         string relativeUrl,
         RequestMethod method,
         CancellationToken cancellationToken,
         IDictionary<string, string> headers = null)
      {
         HttpMessage message = CreateHttpMessage(relativeUrl, method, headers);

         await _httpPipeline.SendAsync(message, cancellationToken).ConfigureAwait(false);

         cancellationToken.ThrowIfCancellationRequested();
         CreateAndThrowIfError(message.Response);

         return await DeserialiseAsync<TResult>(message).ConfigureAwait(false);
      }

      private async Task<(TResult, IDictionary<string, string>)> InvokeExtraAsync<TResult>(
         string relativeUrl,
         RequestMethod method,
         CancellationToken cancellationToken,
         IDictionary<string, string> headers = null)
      {
         HttpMessage message = CreateHttpMessage(relativeUrl, method, headers);

         await _httpPipeline.SendAsync(message, cancellationToken).ConfigureAwait(false);

         cancellationToken.ThrowIfCancellationRequested();
         CreateAndThrowIfError(message.Response);

         TResult ro = await DeserialiseAsync<TResult>(message).ConfigureAwait(false);
         var responseHeaders = new Dictionary<string, string>(StringComparer.InvariantCultureIgnoreCase);
         AddResponseHeaders(responseHeaders, message);
         return (ro, responseHeaders);
      }

      class Void
      {

      }

      private void AddResponseHeaders(IDictionary<string, string> destination, HttpMessage message)
      {
         foreach(HttpHeader header in message.Response.Headers)
         {
            destination[header.Name] = header.Value;
         }
      }

      private void CreateAndThrowIfError(Response response)
      {
         int code = response.Status;
         if(code == 200 || code == 201 || code == 202 || code == 304)
            return;

         response.Headers.TryGetValue(Constants.HeaderNames.ErrorCode, out string errorCode);

         StringBuilder sb = new StringBuilder()
            .Append("Status: ")
            .Append(response.Status)
            .Append(" (")
            .Append(response.ReasonPhrase)
            .AppendLine(")");

         if(!string.IsNullOrEmpty(errorCode))
         {
            sb
               .AppendLine()
               .Append("ErrorCode: ")
               .AppendLine(errorCode);
         }

         sb
            .AppendLine()
            .AppendLine("Headers:");

         foreach(HttpHeader responseHeader in response.Headers)
         {
            sb
                .Append(responseHeader.Name)
                .Append(": ")
                .AppendLine(responseHeader.Value);
         }

         throw new RequestFailedException(
            code,
            sb.ToString(),
            errorCode,
            null);
      }

      private void DecomposePath(string path, out string filesystemName, out string relativePath, bool requireRelativePath = true)
      {
         GenericValidation.CheckBlobFullPath(path);
         string[] parts = StoragePath.Split(path);

         if(requireRelativePath && parts.Length < 2)
         {
            throw new ArgumentException($"path '{path}' must include filesystem name as root folder, i.e. 'filesystem/path'", nameof(path));
         }

         filesystemName = parts[0];

         relativePath = StoragePath.Normalize(StoragePath.Combine(parts.Skip(1)), true);
      }

   }
}
